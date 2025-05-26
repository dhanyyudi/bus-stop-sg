#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SimplyGo Bus Stop Parallel Scraper

This script performs a 2-step process:
1. Extract bus stop codes from the dropdown options in the SimplyGo website
2. Fetch road name and bus description data for each bus code using parallelized Selenium
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import re
import json
import logging
import datetime
import argparse
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from threading import Lock

# Logging configuration
def setup_logging(log_level=logging.INFO):
    """Configure logging with the specified format and level"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Setup file handler
    file_handler = logging.FileHandler(f'logs/simplygo_scraper_{timestamp}.log')
    file_handler.setLevel(log_level)
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Same format for both
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

logger = setup_logging()

# Setup signal handlers for graceful shutdown
shutdown_event = False

def signal_handler(sig, frame):
    """Handler to catch interruption signals (Ctrl+C)"""
    global shutdown_event
    logger.info("Shutdown signal received. Finishing current batch and exiting...")
    shutdown_event = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def extract_bus_codes(url="https://svc.simplygo.com.sg/eservice/eguide/bscode_idx.php"):
    """
    Extract list of bus codes from dropdown options in the SimplyGo website
    
    Args:
        url: SimplyGo website URL
        
    Returns:
        List of bus codes
    """
    # Headers for the request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }
    
    try:
        # Create session and fetch page
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Failed to access page: {response.status_code}")
            return []
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find dropdown element
        # Try both possible names: bs_code or bscode
        select_element = soup.find('select', {'name': 'bs_code'}) or soup.find('select', {'name': 'bscode'})
        
        if not select_element:
            logger.error("Could not find bus code dropdown")
            return []
        
        # Extract all options
        options = select_element.find_all('option')
        
        # Filter valid options (5-digit bus codes)
        bus_codes = []
        for option in options:
            value = option.get('value', '').strip()
            if re.match(r'^\d{5}$', value):
                bus_codes.append(value)
        
        logger.info(f"Extracted {len(bus_codes)} bus codes from dropdown")
        return bus_codes
    
    except Exception as e:
        logger.error(f"Error extracting bus codes: {str(e)}")
        return []

class WebDriverPool:
    """
    WebDriver Pool for use by threads.
    Implements pooling and resource management for WebDriver instances.
    """
    def __init__(self, pool_size=5, headless=True):
        """
        Initialize WebDriver pool
        
        Args:
            pool_size: Number of WebDriver instances in the pool
            headless: Run browser without GUI
        """
        self.pool_size = pool_size
        self.headless = headless
        self.driver_queue = Queue(maxsize=pool_size)
        self.lock = Lock()
        self.initialize_pool()
        
    def initialize_pool(self):
        """Initialize the WebDriver pool with drivers"""
        logger.info(f"Initializing WebDriver pool with {self.pool_size} drivers")
        for _ in range(self.pool_size):
            driver = self._create_driver()
            self.driver_queue.put(driver)
        logger.info("WebDriver pool initialized successfully")
    
    def _create_driver(self):
        """
        Create and return a WebDriver instance
        
        Returns:
            WebDriver instance
        """
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Setup user agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Retry logic for driver creation
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver.set_script_timeout(30)
                driver.set_page_load_timeout(30)
                return driver
            except Exception as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"Failed to create WebDriver on attempt {attempt+1}, retrying... Error: {str(e)}")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to create WebDriver after {max_attempts} attempts: {str(e)}")
                    raise
    
    def get_driver(self, timeout=60):
        """
        Get a WebDriver from the pool
        
        Args:
            timeout: Maximum time to wait for a driver to be available (seconds)
            
        Returns:
            WebDriver instance
        """
        try:
            driver = self.driver_queue.get(timeout=timeout)
            return driver
        except Exception as e:
            logger.error(f"Failed to get driver from pool: {str(e)}")
            # If queue times out, create a new driver
            with self.lock:
                logger.info("Creating a new WebDriver since pool is exhausted")
                return self._create_driver()
    
    def return_driver(self, driver):
        """
        Return a WebDriver to the pool for reuse
        
        Args:
            driver: WebDriver instance
        """
        # If driver crashed or is invalid, create a new one
        try:
            driver.title  # Test if driver is still valid
            try:
                self.driver_queue.put(driver, timeout=1)
            except:
                # If queue is full, close driver
                logger.warning("Driver pool is full, closing driver")
                self._close_driver(driver)
        except:
            logger.warning("Driver is invalid, creating a new one")
            self._close_driver(driver)
            new_driver = self._create_driver()
            self.driver_queue.put(new_driver)
    
    def _close_driver(self, driver):
        """
        Close a WebDriver instance
        
        Args:
            driver: WebDriver instance
        """
        try:
            driver.quit()
        except:
            pass  # Ignore errors during close
            
    def close_all(self):
        """Close all WebDriver instances in the pool"""
        logger.info("Closing all WebDrivers in pool")
        while not self.driver_queue.empty():
            try:
                driver = self.driver_queue.get(timeout=1)
                self._close_driver(driver)
            except:
                pass

def scrape_bus_stop(code, driver_pool, debug=False):
    """
    Scrape bus stop info with a WebDriver from the pool
    
    Args:
        code: Bus stop code
        driver_pool: WebDriverPool instance
        debug: Debug mode
        
    Returns:
        Dictionary with bus stop information
    """
    BASE_URL = "https://svc.simplygo.com.sg/eservice/eguide/bscode_idx.php"
    driver = None
    
    try:
        # Get driver from pool
        driver = driver_pool.get_driver()
        logger.debug(f"Got driver for code {code}")
        
        # Access main page
        driver.get(BASE_URL)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait a bit to ensure JS is loaded
        time.sleep(1)
        
        # Try to fill the form
        try:
            # Find input field
            input_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.NAME, "bscode"))
            )
            input_field.clear()
            input_field.send_keys(code)
            
            # Click Search button
            search_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.NAME, "B1"))
            )
            search_button.click()
        except Exception as e:
            logger.debug(f"Could not find direct input field for {code}: {e}")
            
            # Try using dropdown as alternative
            try:
                select_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.NAME, "bs_code"))
                )
                select = Select(select_element)
                select.select_by_value(code)
                
                # Click Search button
                search_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.NAME, "B1"))
                )
                search_button.click()
            except Exception as e2:
                logger.error(f"Form interaction failed for {code}: {str(e2)}")
                return {'code': code, 'success': False, 'error': f"Form interaction failed: {str(e2)}"}
        
        # Wait for results to load
        time.sleep(3)
        
        # Take screenshot for debug
        if debug:
            os.makedirs('debug', exist_ok=True)
            driver.save_screenshot(f"debug/screenshot_{code}.png")
            logger.debug(f"Saved screenshot to debug/screenshot_{code}.png")
        
        # Get HTML result
        page_source = driver.page_source
        
        # Debug: save HTML
        if debug:
            os.makedirs('debug', exist_ok=True)
            with open(f"debug/bus_stop_{code}.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            logger.debug(f"Saved HTML to debug/bus_stop_{code}.html")
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(page_source, "html.parser")
        
        # Initialize result variables
        road_name = ""
        bus_description = ""
        bus_services = ""
        mrt_lrt_station = ""
        
        # ================= ENHANCED EXTRACTION METHODS =================
        
        # Method 1: Look for text "Searched Result for Bus Stop Code"
        search_result_title = soup.find(string=lambda s: s and "Searched Result for Bus Stop Code" in s)
        
        # Method 2: New way to find result table from page structure
        
        # A. Find tables with specific classes
        main_tables = soup.select("table.maintable") or soup.select("table.tbl") or soup.select("table[width='100%']")
        
        # B. Find tables based on page structure
        # Result tables usually near "Searched Result" text
        tables_after_result = []
        
        if search_result_title:
            # Find element containing search result text
            result_container = search_result_title.parent
            
            # Find all tables below result container
            if result_container:
                tables_after_result = result_container.find_all_next('table')
        
        # Combine all candidate tables
        candidate_tables = list(main_tables) + list(tables_after_result)
        
        # Find table with appropriate headers
        result_table = None
        
        for table in candidate_tables:
            # Check if this table has headers or text related to bus stop data
            if any(term in table.get_text() for term in ['Road Name', 'Bus Stop Description']):
                result_table = table
                break
        
        # If we still haven't found the right table, search based on table structure
        if not result_table:
            for table in soup.find_all('table'):
                rows = table.find_all('tr')
                # Result tables usually have a header row and at least one data row
                if len(rows) >= 2:
                    first_row_cells = rows[0].find_all(['th', 'td'])
                    if len(first_row_cells) >= 2:  # At least 2 columns (road name, description)
                        result_table = table
                        break
        
        # Method 3: Extract data from found table
        if result_table:
            rows = result_table.find_all('tr')
            header_row = None
            data_rows = []
            
            # Identify header row and data rows
            for i, row in enumerate(rows):
                cells = row.find_all(['th', 'td'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                
                # If this row contains headers
                if any('Road Name' in text for text in cell_texts) or any('Bus Stop Description' in text for text in cell_texts):
                    header_row = row
                    # Data rows usually follow header row
                    data_rows = rows[i+1:]
                    break
            
            # If header row found
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
                
                # If there's at least one data row
                if data_rows:
                    first_data_row = data_rows[0]
                    data_cells = first_data_row.find_all(['td'])
                    
                    # Extract data based on header position
                    for i, header in enumerate(headers):
                        if i < len(data_cells):
                            value = data_cells[i].get_text(strip=True)
                            
                            if "Road Name" in header:
                                road_name = value
                            elif "Bus Stop Description" in header:
                                bus_description = value
                            elif "Bus Services" in header:
                                # Bus services could also be separate elements in the cell
                                service_elements = data_cells[i].find_all(['span', 'a', 'div'])
                                
                                if service_elements:
                                    services = [elem.get_text(strip=True) for elem in service_elements if elem.get_text(strip=True)]
                                    bus_services = ', '.join(services)
                                else:
                                    bus_services = value
                            elif "MRT/LRT Station" in header:
                                mrt_lrt_station = value
            
            # If we didn't find data with the above approach, try an alternative approach
            if not road_name and not bus_description:
                # Look for data in two-column format (label:value)
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        if "Road Name" in label:
                            road_name = value
                        
                        if "Bus Stop Description" in label:
                            bus_description = value
        
        # Method 4: Using JavaScript Executor to extract data directly
        if not road_name or not bus_description:
            try:
                # Try extracting data using JavaScript executor
                # This can work better for elements that might be hidden or dynamic
                
                # Search for road name data
                js_road_name = """
                    var roadNameElements = Array.from(document.querySelectorAll('th, td')).filter(el => el.innerText.includes('Road Name'));
                    if (roadNameElements.length > 0) {
                        var nextElement = roadNameElements[0].nextElementSibling;
                        return nextElement ? nextElement.innerText.trim() : '';
                    }
                    return '';
                """
                road_name_js = driver.execute_script(js_road_name)
                if road_name_js and not road_name:
                    road_name = road_name_js
                
                # Search for bus description data
                js_bus_desc = """
                    var descElements = Array.from(document.querySelectorAll('th, td')).filter(el => el.innerText.includes('Bus Stop Description'));
                    if (descElements.length > 0) {
                        var nextElement = descElements[0].nextElementSibling;
                        return nextElement ? nextElement.innerText.trim() : '';
                    }
                    return '';
                """
                bus_desc_js = driver.execute_script(js_bus_desc)
                if bus_desc_js and not bus_description:
                    bus_description = bus_desc_js
                
                # Try alternative way to find data
                js_alternative = """
                    // Get all table rows
                    var rows = document.querySelectorAll('tr');
                    var data = {roadName: '', busDescription: '', busServices: ''};
                    
                    // Check each row for data
                    for (var i = 0; i < rows.length; i++) {
                        var cells = rows[i].querySelectorAll('td, th');
                        
                        // Check if this is a header row
                        var headerTexts = Array.from(cells).map(c => c.innerText.trim());
                        if (headerTexts.some(t => t.includes('Road Name') || t.includes('Bus Stop Description'))) {
                            // Next row likely contains data
                            if (i + 1 < rows.length) {
                                var dataCells = rows[i+1].querySelectorAll('td');
                                
                                // Map headers to data cells
                                for (var j = 0; j < headerTexts.length; j++) {
                                    if (j < dataCells.length) {
                                        if (headerTexts[j].includes('Road Name')) {
                                            data.roadName = dataCells[j].innerText.trim();
                                        } else if (headerTexts[j].includes('Bus Stop Description')) {
                                            data.busDescription = dataCells[j].innerText.trim();
                                        } else if (headerTexts[j].includes('Bus Services')) {
                                            data.busServices = dataCells[j].innerText.trim();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    return data;
                """
                alternative_data = driver.execute_script(js_alternative)
                
                if alternative_data:
                    if not road_name and alternative_data.get('roadName'):
                        road_name = alternative_data['roadName']
                    
                    if not bus_description and alternative_data.get('busDescription'):
                        bus_description = alternative_data['busDescription']
                    
                    if not bus_services and alternative_data.get('busServices'):
                        bus_services = alternative_data['busServices']
            
            except Exception as js_error:
                logger.debug(f"Error executing JavaScript for {code}: {str(js_error)}")
        
        # Method 5: Visual search using Selenium
        if not road_name or not bus_description:
            try:
                # Find all table elements on the page
                tables = driver.find_elements(By.TAG_NAME, 'table')
                
                for table in tables:
                    # Check if this table contains the data we're looking for
                    if 'Road Name' in table.text or 'Bus Stop Description' in table.text:
                        # Find rows in the table
                        rows = table.find_elements(By.TAG_NAME, 'tr')
                        
                        # Identify header and data
                        for i, row in enumerate(rows):
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            cell_texts = [cell.text for cell in cells]
                            
                            # Check label-value pairs
                            if len(cells) >= 2:
                                if 'Road Name' in cells[0].text and not road_name:
                                    road_name = cells[1].text.strip()
                                
                                if 'Bus Stop Description' in cells[0].text and not bus_description:
                                    bus_description = cells[1].text.strip()
                            
                            # Check if this is a data row below a header row
                            if i > 0 and len(cells) >= 2:
                                # Try to look at header row
                                header_row = rows[i-1]
                                headers = header_row.find_elements(By.TAG_NAME, 'th')
                                
                                if headers:
                                    header_texts = [h.text for h in headers]
                                    
                                    # Map headers to data cells
                                    for j, header in enumerate(header_texts):
                                        if j < len(cells):
                                            if 'Road Name' in header and not road_name:
                                                road_name = cells[j].text.strip()
                                            
                                            if 'Bus Stop Description' in header and not bus_description:
                                                bus_description = cells[j].text.strip()
                
                # Try alternative method: finding elements specifically
                if not road_name:
                    road_elements = driver.find_elements(By.XPATH, "//td[preceding-sibling::td[contains(text(), 'Road Name')]]")
                    if road_elements:
                        road_name = road_elements[0].text.strip()
                
                if not bus_description:
                    desc_elements = driver.find_elements(By.XPATH, "//td[preceding-sibling::td[contains(text(), 'Bus Stop Description')]]")
                    if desc_elements:
                        bus_description = desc_elements[0].text.strip()
            
            except Exception as selenium_error:
                logger.debug(f"Error using Selenium element finder for {code}: {str(selenium_error)}")
        
        # Method 6: Data validation and final transformation
        
        # 1. Validation - ensure we don't get labels as values
        if road_name in ["Road Name", "Bus Stop Description"]:
            logger.warning(f"WARNING: Road Name has invalid value for {code} (it contains column name)")
            road_name = ""
        
        if bus_description in ["Road Name", "Bus Stop Description"]:
            logger.warning(f"WARNING: Bus Description has invalid value for {code} (it contains column name)")
            bus_description = ""
        
        # 2. Clean values - remove excess whitespace and unwanted characters
        if road_name:
            road_name = road_name.strip()
        
        if bus_description:
            bus_description = bus_description.strip()
        
        if bus_services:
            bus_services = bus_services.strip()
        
        # Result
        result = {
            'code': code,
            'road_name': road_name,
            'bus_description': bus_description,
            'bus_services': bus_services,
            'mrt_lrt_station': mrt_lrt_station,
            'success': True if (road_name or bus_description) else False,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        logger.debug(f"Completed scraping for code {code}: success={result['success']}")
        return result
    
    except Exception as e:
        logger.error(f"Error scraping bus stop {code}: {str(e)}")
        return {'code': code, 'success': False, 'error': str(e), 'timestamp': datetime.datetime.now().isoformat()}
    
    finally:
        # Return driver to pool
        if driver:
            driver_pool.return_driver(driver)
            logger.debug(f"Returned driver to pool for code {code}")

def scrape_parallel(codes, n_workers=5, batch_size=20, debug=False):
    """
    Scrape batch of bus codes in parallel
    
    Args:
        codes: List of bus codes
        n_workers: Number of parallel threads
        batch_size: Batch size for saving progress
        debug: Debug mode
        
    Returns:
        List of scraping results
    """
    results = []
    start_time = time.time()
    checkpoint_time = start_time
    
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    # Timestamp for file name
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"output/simplygo_bus_stops_{timestamp}.csv"
    progress_file = f"output/progress_{timestamp}.json"
    
    # Setup driver pool
    logger.info(f"Setting up WebDriver pool with {n_workers} workers")
    driver_pool = WebDriverPool(pool_size=n_workers)
    
    try:
        # Setup ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            # Submit all tasks
            future_to_code = {}
            
            for code in codes:
                # Add jitter to avoid all threads hitting the server at the same time
                time.sleep(random.uniform(0.1, 0.5))
                future = executor.submit(scrape_bus_stop, code, driver_pool, debug)
                future_to_code[future] = code
            
            # Process results as they complete
            completed = 0
            total = len(codes)
            
            # Setup progress bar
            with tqdm(total=total, desc="Scraping progress") as pbar:
                for future in as_completed(future_to_code):
                    if shutdown_event:
                        logger.info("Shutdown detected. Cancelling remaining tasks...")
                        break
                    
                    code = future_to_code[future]
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # Update progress bar
                        completed += 1
                        pbar.update(1)
                        
                        # Logging
                        success_status = "Success" if result.get('success', False) else "Failed"
                        logger.debug(f"[{completed}/{total}] {success_status} for code {code}")
                        
                        # Save checkpoint if batch_size reached or every 5 minutes
                        current_time = time.time()
                        if completed % batch_size == 0 or (current_time - checkpoint_time) > 300:  # 5 minutes
                            checkpoint_time = current_time
                            
                            # Save progress
                            df_progress = pd.DataFrame(results)
                            progress_file_batch = f"output/progress_{timestamp}_{completed}.csv"
                            df_progress.to_csv(progress_file_batch, index=False)
                            
                            # Save state for resume
                            progress_state = {
                                'completed': [r['code'] for r in results],
                                'remaining': [c for c in codes if c not in [r['code'] for r in results]],
                                'timestamp': datetime.datetime.now().isoformat(),
                                'total': total,
                                'progress': f"{completed}/{total} ({completed/total*100:.1f}%)"
                            }
                            
                            with open(progress_file, 'w') as f:
                                json.dump(progress_state, f, indent=2)
                            
                            logger.info(f"Checkpoint saved at {completed}/{total} ({completed/total*100:.1f}%)")
                    
                    except Exception as e:
                        logger.error(f"Error processing result for code {code}: {str(e)}")
                        # Add failed result
                        results.append({
                            'code': code,
                            'success': False,
                            'error': str(e),
                            'timestamp': datetime.datetime.now().isoformat()
                        })
                        pbar.update(1)
    
    finally:
        # Close all WebDrivers
        driver_pool.close_all()
    
    # Calculate statistics
    end_time = time.time()
    total_time = end_time - start_time
    success_count = sum(1 for r in results if r.get('success', False))
    
    logger.info(f"Scraping completed in {total_time:.2f} seconds")
    logger.info(f"Success rate: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
    
    # Save final results
    if results:
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    return results

def resume_scraping(progress_file):
    """
    Resume scraping from progress file
    
    Args:
        progress_file: Path to JSON progress file
        
    Returns:
        List of scraping results
    """
    if not os.path.exists(progress_file):
        logger.error(f"Progress file {progress_file} not found!")
        return []
    
    try:
        # Load progress state
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        
        remaining_codes = progress.get('remaining', [])
        completed_codes = progress.get('completed', [])
        
        if not remaining_codes:
            logger.info("No remaining codes to process!")
            return []
        
        logger.info(f"Resuming scraping with {len(remaining_codes)} remaining codes")
        logger.info(f"Previously completed: {len(completed_codes)} codes")
        
        # Load existing results if available
        timestamp = progress_file.split('_')[1].split('.')[0]
        existing_results = []
        
        # Find the latest progress CSV
        progress_csv_files = [f for f in os.listdir('output') if f.startswith(f'progress_{timestamp}_') and f.endswith('.csv')]
        
        if progress_csv_files:
            # Sort by the number in the filename to get the latest
            latest_file = sorted(progress_csv_files, key=lambda x: int(x.split('_')[-1].split('.')[0]))[-1]
            file_path = os.path.join('output', latest_file)
            
            logger.info(f"Loading existing results from {file_path}")
            df_existing = pd.read_csv(file_path)
            existing_results = df_existing.to_dict('records')
        
        # Continue scraping remaining codes
        new_results = scrape_parallel(remaining_codes)
        
        # Combine results
        all_results = existing_results + new_results
        
        return all_results
    
    except Exception as e:
        logger.error(f"Error resuming scraping: {str(e)}")
        return []

def analyze_results(csv_file):
    """
    Analyze scraping results
    
    Args:
        csv_file: CSV file with results
    """
    if not os.path.exists(csv_file):
        logger.error(f"File {csv_file} not found!")
        return
    
    try:
        df = pd.read_csv(csv_file)
        
        # Basic statistics
        total_records = len(df)
        success_count = df['success'].sum() if 'success' in df.columns else 0
        road_name_count = df['road_name'].notna().sum()
        desc_count = df['bus_description'].notna().sum()
        
        print(f"=== Analysis for {csv_file} ===")
        print(f"Total records: {total_records}")
        print(f"Success count: {success_count} ({success_count/total_records*100:.1f}%)")
        print(f"Records with road name: {road_name_count} ({road_name_count/total_records*100:.1f}%)")
        print(f"Records with bus description: {desc_count} ({desc_count/total_records*100:.1f}%)")
        
        # Sample data
        print("\nSample successful records:")
        success_samples = df[df['success'] == True].head(5) if 'success' in df.columns else df.head(5)
        print(success_samples[['code', 'road_name', 'bus_description']])
        
        # Error samples if any
        if 'error' in df.columns and df['error'].notna().any():
            print("\nSample error records:")
            error_samples = df[df['error'].notna()].head(5)
            print(error_samples[['code', 'error']])
        
        # Save analysis
        analysis_file = csv_file.replace('.csv', '_analysis.txt')
        with open(analysis_file, 'w') as f:
            f.write(f"=== Analysis for {csv_file} ===\n")
            f.write(f"Total records: {total_records}\n")
            f.write(f"Success count: {success_count} ({success_count/total_records*100:.1f}%)\n")
            f.write(f"Records with road name: {road_name_count} ({road_name_count/total_records*100:.1f}%)\n")
            f.write(f"Records with bus description: {desc_count} ({desc_count/total_records*100:.1f}%)\n")
        
        print(f"Analysis saved to {analysis_file}")
    
    except Exception as e:
        logger.error(f"Error analyzing results: {str(e)}")

def main():
    """Main function to run the scraper"""
    parser = argparse.ArgumentParser(description='SimplyGo Bus Stop Parallel Scraper')
    parser.add_argument('--extract', action='store_true', help='Extract bus codes only')
    parser.add_argument('--scrape', action='store_true', help='Scrape bus stops')
    parser.add_argument('--analyze', type=str, help='Analyze CSV results file')
    parser.add_argument('--resume', type=str, help='Resume scraping from progress file')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    parser.add_argument('--batch-size', type=int, default=20, help='Batch size for saving progress')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--input', type=str, help='Input CSV file with bus codes')
    parser.add_argument('--limit', type=int, help='Limit number of codes to process')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    
    args = parser.parse_args()
    
    # Configure log level
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)
    
    if args.extract:
        # Extract bus codes
        bus_codes = extract_bus_codes()
        
        if bus_codes:
            # Save to CSV
            df_codes = pd.DataFrame({'code': bus_codes})
            
            os.makedirs('output', exist_ok=True)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            codes_csv_path = f'output/bus_codes_{timestamp}.csv'
            df_codes.to_csv(codes_csv_path, index=False)
            
            logger.info(f"Saved {len(df_codes)} bus codes to {codes_csv_path}")
        else:
            logger.error("Failed to extract bus codes")
    
    elif args.scrape:
        # Scrape bus stops
        if args.input:
            # Load codes from input file
            if not os.path.exists(args.input):
                logger.error(f"Input file {args.input} not found!")
                return
            
            df_input = pd.read_csv(args.input)
            if 'code' not in df_input.columns:
                logger.error("Input file must have a 'code' column")
                return
            
            bus_codes = df_input['code'].astype(str).tolist()
        else:
            # Extract codes from website
            bus_codes = extract_bus_codes()
        
        if not bus_codes:
            logger.error("No bus codes to process!")
            return
        
        # Limit if requested
        if args.limit and args.limit > 0:
            logger.info(f"Limiting to first {args.limit} codes")
            bus_codes = bus_codes[:args.limit]
        
        logger.info(f"Starting parallel scraping with {len(bus_codes)} codes and {args.workers} workers")
        results = scrape_parallel(bus_codes, n_workers=args.workers, batch_size=args.batch_size, debug=args.debug)
        
        logger.info(f"Scraping completed. Processed {len(results)} codes.")
    
    elif args.resume:
        # Resume scraping
        logger.info(f"Resuming scraping from {args.resume}")
        results = resume_scraping(args.resume)
        
        logger.info(f"Resume scraping completed. Processed {len(results)} codes.")
    
    elif args.analyze:
        # Analyze results
        logger.info(f"Analyzing results from {args.analyze}")
        analyze_results(args.analyze)
    
    else:
        parser.print_help()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Script execution finished")
