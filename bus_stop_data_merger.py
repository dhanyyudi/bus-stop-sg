from parallelized_simplygo_scraper import scrape_parallel
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bus Stop Data Collector and Merger (Optimized)

This script performs a 4-step process:
1. Download data from LTA DataMall API
2. Compare with previous LTA DataMall data to identify differences
3. Download data from SimplyGo only for new/modified bus stops
4. Merge data from both sources to create a corrected dataset

Author: Dhany Yudi Prasetyo
Date: May 2025
"""

import os
import sys
import time
import random
import json
import datetime
import logging
import argparse
import pandas as pd
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from threading import Lock
from pathlib import Path
import glob
import subprocess
import re

# Configure logging
def setup_logging(log_level=logging.INFO):
    """Configure logging with the specified format and level"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Setup file handler
    file_handler = logging.FileHandler(f'logs/bus_data_collector_{timestamp}.log')
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

# Initialize logger
logger = logging.getLogger(__name__)

class PerformanceTimer:
    """Simple timer class for performance measurement"""
    def __init__(self, name="Operation"):
        self.name = name
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        logger.info(f"{self.name} completed in {elapsed:.2f} seconds")

def download_lta_datamall(api_key, email=None, password=None, output_file=None):
    """
    Download bus stop data from LTA DataMall API
    
    Args:
        api_key: LTA DataMall API key
        email: LTA DataMall email (optional)
        password: LTA DataMall password (optional)
        output_file: Path to save CSV output
        
    Returns:
        DataFrame with LTA DataMall data
    """
    with PerformanceTimer("LTA DataMall download"):
        logger.info("Starting download from LTA DataMall...")
        
        df = pd.DataFrame(columns=['code', 'name', 'street', 'lat', 'lon'])
        
        i = 0
        total_records = 0
        
        try:
            while True:
                url = f"http://datamall2.mytransport.sg/ltaodataservice/BusStops?$skip={i}"
                
                # Setup headers with API key
                headers = {
                    'AccountKey': api_key,
                    'Content-Type': 'application/json'
                }
                
                # Setup payload with email and password if provided
                payload = None
                if email and password:
                    payload = json.dumps({"email": email, "password": password})
                    logger.debug("Using email and password for authentication")
                
                # Add delay to respect API rate limits
                if i > 0:
                    time.sleep(1)
                
                # Make the API request
                try:
                    with PerformanceTimer(f"API request (skip={i})"):
                        if payload:
                            response = requests.request("GET", url, headers=headers, data=payload, timeout=30)
                        else:
                            response = requests.request("GET", url, headers=headers, timeout=30)
                    
                    if response.status_code != 200:
                        logger.error(f"API request failed with status code {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        break
                        
                    data = response.json()
                    
                    if len(data['value']) == 0:
                        logger.info("No more records to fetch from API")
                        break
                    else:
                        batch_size = len(data['value'])
                        total_records += batch_size
                        logger.info(f"Retrieved {batch_size} records (total: {total_records})")
                        
                        for j in range(batch_size):
                            code = data['value'][j]['BusStopCode']
                            name = data['value'][j]['Description']
                            street = data['value'][j]['RoadName']
                            lat = data['value'][j]['Latitude']
                            lon = data['value'][j]['Longitude']
                            
                            index = len(df)
                            df.loc[index, 'code'] = code
                            df.loc[index, 'name'] = name
                            df.loc[index, 'street'] = street
                            df.loc[index, 'lat'] = lat
                            df.loc[index, 'lon'] = lon
                    
                    # Move to the next page
                    i += 500
                    
                except requests.exceptions.RequestException as req_err:
                    logger.error(f"Request error: {str(req_err)}")
                    # Try a few more times with backoff
                    retry_count = 0
                    max_retries = 3
                    while retry_count < max_retries:
                        retry_count += 1
                        backoff_time = retry_count * 5  # 5, 10, 15 seconds
                        logger.info(f"Retrying in {backoff_time} seconds... (attempt {retry_count}/{max_retries})")
                        time.sleep(backoff_time)
                        
                        try:
                            if payload:
                                response = requests.request("GET", url, headers=headers, data=payload, timeout=30)
                            else:
                                response = requests.request("GET", url, headers=headers, timeout=30)
                            
                            if response.status_code == 200:
                                logger.info("Retry successful")
                                data = response.json()
                                # Process data as before
                                if len(data['value']) == 0:
                                    logger.info("No more records to fetch from API")
                                    i = None  # Set to None to exit the loop
                                    break
                                else:
                                    batch_size = len(data['value'])
                                    total_records += batch_size
                                    logger.info(f"Retrieved {batch_size} records (total: {total_records})")
                                    
                                    for j in range(batch_size):
                                        code = data['value'][j]['BusStopCode']
                                        name = data['value'][j]['Description']
                                        street = data['value'][j]['RoadName']
                                        lat = data['value'][j]['Latitude']
                                        lon = data['value'][j]['Longitude']
                                        
                                        index = len(df)
                                        df.loc[index, 'code'] = code
                                        df.loc[index, 'name'] = name
                                        df.loc[index, 'street'] = street
                                        df.loc[index, 'lat'] = lat
                                        df.loc[index, 'lon'] = lon
                                    
                                    # Move to the next page
                                    i += 500
                                    break  # Exit retry loop
                            else:
                                logger.error(f"Retry failed with status code {response.status_code}")
                        except Exception as retry_err:
                            logger.error(f"Retry error: {str(retry_err)}")
                    
                    if retry_count >= max_retries:
                        logger.error("Maximum retry attempts reached. Aborting.")
                        break
        
        except Exception as e:
            logger.error(f"Error downloading LTA DataMall data: {str(e)}")
            logger.error(traceback.format_exc())
        
        # Save to file if output_file is provided
        if output_file and not df.empty:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
            
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} records to {output_file}")
        
        return df

def get_latest_lta_file(data_dir="data", current_file=None):
    """
    Find the latest LTA DataMall file in the data directory, excluding current file
    
    Args:
        data_dir: Directory to search for files
        current_file: Current file to exclude from search
        
    Returns:
        Path to the latest file, or None if no files found
    """
    pattern = os.path.join(data_dir, "LTA_bus_stops_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        logger.info("No previous LTA DataMall files found")
        return None
    
    # Filter out current file if provided
    if current_file:
        current_base_name = os.path.basename(current_file)
        files = [f for f in files if os.path.basename(f) != current_base_name]
    
    if not files:
        logger.info("No previous LTA DataMall files found (excluding current)")
        return None
    
    # Sort by modification time (newest first)
    files.sort(key=os.path.getmtime, reverse=True)
    latest_file = files[0]
    
    logger.info(f"Found latest LTA DataMall file: {latest_file}")
    return latest_file

def get_previous_lta_file(current_date, data_dir="data"):
    """
    Find LTA DataMall file from a previous date
    
    Args:
        current_date: Current date string (DDMMYYYY)
        data_dir: Directory to search for files
        
    Returns:
        Path to the previous file, or None if no files found
    """
    pattern = os.path.join(data_dir, "LTA_bus_stops_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        logger.info("No previous LTA DataMall files found")
        return None
    
    # Filter out files with current date
    previous_files = []
    for file in files:
        filename = os.path.basename(file)
        date_match = re.search(r'LTA_bus_stops_(\d{8})\.csv', filename)
        if date_match and date_match.group(1) != current_date:
            previous_files.append(file)
    
    if not previous_files:
        logger.info("No previous LTA DataMall files found (from different dates)")
        return None
    
    # Sort by date (newest first)
    previous_files.sort(reverse=True)
    latest_file = previous_files[0]
    
    logger.info(f"Found previous LTA DataMall file: {latest_file}")
    return latest_file

def compare_lta_data(new_df, old_file, output_diff_file=None):
    """
    Compare new LTA data with previous data to find differences
    
    Args:
        new_df: DataFrame with new LTA data
        old_file: Path to the previous LTA data file
        output_diff_file: Path to save differences CSV output
        
    Returns:
        DataFrame with differences only
    """
    with PerformanceTimer("LTA data comparison"):
        logger.info(f"Comparing new LTA data with previous data from {old_file}")
        
        try:
            # Load previous data
            old_df = pd.read_csv(old_file)
            
            # Ensure 'code' column is string type for proper comparison
            old_df['code'] = old_df['code'].astype(str)
            new_df['code'] = new_df['code'].astype(str)
            
            # Find new bus stops (codes in new_df but not in old_df)
            new_codes = set(new_df['code']) - set(old_df['code'])
            new_stops = new_df[new_df['code'].isin(new_codes)]
            
            # Find modified bus stops (same code but different data)
            common_codes = set(new_df['code']) & set(old_df['code'])
            
            # Create a merged dataframe for comparison
            old_subset = old_df[old_df['code'].isin(common_codes)].copy()
            new_subset = new_df[new_df['code'].isin(common_codes)].copy()
            
            # Set index for easier comparison
            old_subset.set_index('code', inplace=True)
            new_subset.set_index('code', inplace=True)
            
            # Find rows that have changed
            changed_mask = (old_subset['name'] != new_subset['name']) | \
                          (old_subset['street'] != new_subset['street']) | \
                          (old_subset['lat'] != new_subset['lat']) | \
                          (old_subset['lon'] != new_subset['lon'])
            
            # Get codes of changed rows
            changed_codes = set(changed_mask[changed_mask].index)
            
            # Reset index
            old_subset.reset_index(inplace=True)
            new_subset.reset_index(inplace=True)
            
            # Extract changed stops from new data
            changed_stops = new_df[new_df['code'].isin(changed_codes)]
            
            # Combine new and changed stops
            diff_df = pd.concat([new_stops, changed_stops], ignore_index=True)
            
            # Add a column to indicate if it's a new or changed stop
            diff_df['change_type'] = 'changed'
            diff_df.loc[diff_df['code'].isin(new_codes), 'change_type'] = 'new'
            
            # Get counts
            new_count = len(new_codes)
            changed_count = len(changed_codes)
            total_diff_count = len(diff_df)
            
            logger.info(f"Found {new_count} new bus stops and {changed_count} modified bus stops")
            logger.info(f"Total differences: {total_diff_count} bus stops")
            
            # Save to file if output_diff_file is provided
            if output_diff_file and not diff_df.empty:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_diff_file) if os.path.dirname(output_diff_file) else '.', exist_ok=True)
                
                diff_df.to_csv(output_diff_file, index=False)
                logger.info(f"Saved {len(diff_df)} differences to {output_diff_file}")
            
            return diff_df
        
        except Exception as e:
            logger.error(f"Error comparing LTA data: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()  # Return empty DataFrame on error

class WebDriverPool:
    """
    WebDriver Pool for use by threads.
    Implements pooling and resource management for WebDriver instances.
    """
    def __init__(self, pool_size=4, headless=True):
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
        with PerformanceTimer("WebDriver pool initialization"):
            # Initialize drivers one by one and measure time
            driver_created = False
            
            for i in range(self.pool_size):
                try:
                    logger.info(f"Creating WebDriver {i+1}/{self.pool_size}")
                    start_time = time.time()
                    driver = self._create_driver()
                    creation_time = time.time() - start_time
                    logger.info(f"WebDriver {i+1} created in {creation_time:.2f} seconds")
                    
                    # Test the driver with a simple request
                    try:
                        logger.info(f"Testing WebDriver {i+1}")
                        driver.set_page_load_timeout(30)
                        driver.get("https://www.google.com")
                        logger.info(f"WebDriver {i+1} navigation test successful (title: {driver.title})")
                    except Exception as e:
                        logger.warning(f"WebDriver {i+1} test failed: {str(e)}")
                    
                    self.driver_queue.put(driver)
                    driver_created = True
                except Exception as e:
                    logger.error(f"Failed to create WebDriver {i+1}: {str(e)}")
                    # Continue attempting to create at least one driver
                
            if not driver_created:
                logger.warning("Could not create any WebDrivers! The scraping functionality will be skipped.")
                # We'll handle this scenario in the scraping functions
                
        logger.info("WebDriver pool initialization completed")
    
    def _create_driver(self):
        """
        Create and return a WebDriver instance
        
        Returns:
            WebDriver instance
        """
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")  # Updated headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        chrome_options.add_argument("--no-sandbox")  # Required for GitHub Actions
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-features=NetworkService')  # Try to improve network stability
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Add performance logging
        chrome_options.add_argument('--enable-logging')
        chrome_options.add_argument('--v=1')
        
        # Setup user agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Retry logic for driver creation
        max_attempts = 5  # Increased from 3 to 5
        for attempt in range(max_attempts):
            try:
                # Try different approaches to get ChromeDriver
                try:
                    # Method 1: Use ChromeDriverManager with specific version
                    CHROMEDRIVER_VERSION = "114.0.5735.90"  # This is a known stable version
                    service = Service(ChromeDriverManager(driver_version=CHROMEDRIVER_VERSION).install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    logger.info("Successfully created ChromeDriver using ChromeDriverManager with specific version")
                except Exception as e1:
                    logger.warning(f"First ChromeDriver approach failed: {str(e1)}, trying alternate method")
                    
                    try:
                        # Method 2: Use ChromeDriverManager without version
                        service = Service(ChromeDriverManager().install())
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                        logger.info("Successfully created ChromeDriver using default ChromeDriverManager")
                    except Exception as e2:
                        logger.warning(f"Second ChromeDriver approach failed: {str(e2)}, trying system chromedriver")
                        
                        try:
                            # Method 3: Try with system chromedriver
                            # Check if chromedriver exists
                            try:
                                result = subprocess.run(["which", "chromedriver"], capture_output=True, text=True, check=False)
                                chromedriver_path = result.stdout.strip()
                                
                                if chromedriver_path:
                                    logger.info(f"Found system chromedriver at {chromedriver_path}")
                                    service = Service(executable_path=chromedriver_path)
                                    driver = webdriver.Chrome(service=service, options=chrome_options)
                                    logger.info("Successfully created ChromeDriver using system chromedriver")
                                else:
                                    raise FileNotFoundError("System chromedriver not found")
                            except:
                                # If 'which' command failed, try direct path
                                service = Service(executable_path="/usr/bin/chromedriver")
                                driver = webdriver.Chrome(service=service, options=chrome_options)
                                logger.info("Successfully created ChromeDriver using direct path")
                        except Exception as e3:
                            logger.warning(f"System chromedriver failed: {str(e3)}, trying with no service")
                            
                            # Method 4: Last resort - try without service
                            driver = webdriver.Chrome(options=chrome_options)
                            logger.info("Successfully created ChromeDriver without service specification")
                
                driver.set_script_timeout(60)  # Increased from 30 to 60
                driver.set_page_load_timeout(60)  # Increased from 30 to 60
                
                # Add a custom property to track usage
                driver._usage_count = 0
                driver._creation_time = time.time()
                
                return driver
            except Exception as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"Failed to create WebDriver on attempt {attempt+1}, retrying in {(attempt+1)*5} seconds... Error: {str(e)}")
                    time.sleep((attempt+1) * 5)  # Increased backoff time
                else:
                    logger.error(f"Failed to create WebDriver after {max_attempts} attempts: {str(e)}")
                    raise
    
    def get_driver(self, timeout=120):  # Increased timeout from 60 to 120
        """
        Get a WebDriver from the pool
        
        Args:
            timeout: Maximum time to wait for a driver to be available (seconds)
            
        Returns:
            WebDriver instance
        """
        try:
            start_time = time.time()
            logger.debug(f"Waiting for an available WebDriver (timeout: {timeout}s)")
            
            driver = self.driver_queue.get(timeout=timeout)
            wait_time = time.time() - start_time
            
            # Check if driver is still valid, recreate if necessary
            try:
                if hasattr(driver, '_usage_count'):
                    driver._usage_count += 1
                    age = time.time() - driver._creation_time
                    logger.debug(f"Got WebDriver (usage: {driver._usage_count}, age: {age:.1f}s) after waiting {wait_time:.2f}s")
                    
                    # Recreate driver if it's been used too many times or is too old
                    if driver._usage_count > 50 or age > 1800:  # 30 minutes
                        logger.info(f"Recreating WebDriver (usage: {driver._usage_count}, age: {age:.1f}s)")
                        self._close_driver(driver)
                        driver = self._create_driver()
                else:
                    logger.debug(f"Got WebDriver after waiting {wait_time:.2f}s")
                
                # Test if driver is responsive
                driver.title  # This will throw if driver is not responsive
                return driver
                
            except Exception as e:
                logger.warning(f"WebDriver validation failed, creating a new one: {str(e)}")
                try:
                    self._close_driver(driver)
                except:
                    pass
                
                # Create a new driver
                with self.lock:  # Lock to prevent multiple threads creating drivers simultaneously
                    return self._create_driver()
                
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
            # Clean up browser state
            try:
                driver.delete_all_cookies()
                driver.execute_script("window.localStorage.clear();")
                driver.execute_script("window.sessionStorage.clear();")
            except:
                pass  # Ignore errors during cleanup
                
            # Test if driver is still valid
            driver.title  # This will throw if driver is not responsive
            
            try:
                self.driver_queue.put(driver, timeout=1)
                logger.debug("Returned WebDriver to pool")
            except:
                # If queue is full, close driver
                logger.warning("Driver pool is full, closing driver")
                self._close_driver(driver)
        except:
            logger.warning("Driver is invalid, creating a new one")
            self._close_driver(driver)
            try:
                new_driver = self._create_driver()
                self.driver_queue.put(new_driver)
            except Exception as e:
                logger.error(f"Failed to create replacement driver: {str(e)}")
    
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
        closed_count = 0
        while not self.driver_queue.empty():
            try:
                driver = self.driver_queue.get(timeout=1)
                self._close_driver(driver)
                closed_count += 1
            except:
                pass
        logger.info(f"Closed {closed_count} WebDrivers")

    def warmup(self, url="https://svc.simplygo.com.sg/eservice/eguide/bscode_idx.php"):
        """
        Warmup all WebDrivers in the pool
        
        Args:
            url: URL to navigate to for warmup
            
        Returns:
            Number of successful warmups
        """
        logger.info(f"Warming up WebDriver pool with URL: {url}")
        success_count = 0
        
        # Get all drivers from the queue
        drivers = []
        while not self.driver_queue.empty():
            try:
                drivers.append(self.driver_queue.get(timeout=1))
            except:
                pass
        
        logger.info(f"Got {len(drivers)} drivers for warmup")
        
        # Warmup each driver
        for i, driver in enumerate(drivers):
            try:
                logger.info(f"Warming up WebDriver {i+1}/{len(drivers)}")
                start_time = time.time()
                driver.get(url)
                warmup_time = time.time() - start_time
                logger.info(f"WebDriver {i+1} warmup successful in {warmup_time:.2f}s (page title: {driver.title})")
                success_count += 1
            except Exception as e:
                logger.warning(f"WebDriver {i+1} warmup failed: {str(e)}")
                # Try to recreate the driver
                try:
                    self._close_driver(driver)
                    driver = self._create_driver()
                    logger.info(f"Recreated WebDriver {i+1}")
                except Exception as e2:
                    logger.error(f"Failed to recreate WebDriver {i+1}: {str(e2)}")
                    driver = None
            
            # Return the driver to the queue
            if driver:
                self.driver_queue.put(driver)
        
        logger.info(f"Warmup completed: {success_count}/{len(drivers)} successful")
        return success_count
    
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
    start_time = time.time()  # Track start time for performance monitoring
    
    # Check if driver pool is completely empty
    if driver_pool.driver_queue.empty():
        logger.warning(f"No WebDriver available for code {code}, skipping")
        return {
            'code': code, 
            'success': False, 
            'error': "No WebDriver available", 
            'timestamp': datetime.datetime.now().isoformat(),
            'processing_time': 0
        }
    
    try:
        # Get driver from pool
        driver = driver_pool.get_driver()
        logger.debug(f"Got driver for code {code}")
        
        # Access main page
        try:
            driver.get(BASE_URL)
            
            # Wait for page to load with increased timeout and better error handling
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for page load for code {code}, retrying...")
                driver.refresh()  # Refresh page
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            
            logger.debug(f"Page loaded for code {code} in {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"Error loading page for code {code}: {str(e)}")
            return {'code': code, 'success': False, 'error': f"Page load error: {str(e)}", 'timestamp': datetime.datetime.now().isoformat(), 'processing_time': time.time() - start_time}
        
        # Wait for all elements to be fully loaded
        time.sleep(1)
        
        # Try to fill form
        form_start = time.time()
        try:
            # Find input field
            input_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "bscode"))
            )
            input_field.clear()
            input_field.send_keys(code)
            
            # Click Search button
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.NAME, "B1"))
            )
            search_button.click()
            logger.debug(f"Form submitted for code {code} in {time.time() - form_start:.2f}s")
        except Exception as e:
            logger.debug(f"Could not find direct input field for {code}, trying alternative: {e}")
            
            # Try using dropdown as alternative
            try:
                select_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.NAME, "bs_code"))
                )
                select = Select(select_element)
                select.select_by_value(code)
                
                # Click Search button
                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.NAME, "B1"))
                )
                search_button.click()
                logger.debug(f"Alternative form submitted for code {code} in {time.time() - form_start:.2f}s")
            except Exception as e2:
                logger.error(f"Form interaction failed for {code}: {str(e2)}")
                return {'code': code, 'success': False, 'error': f"Form interaction failed: {str(e2)}", 'timestamp': datetime.datetime.now().isoformat(), 'processing_time': time.time() - start_time}
        
        # Wait for results to load with timeout
        results_start = time.time()
        try:
            # First try to wait for search result specific elements
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: "Searched Result" in d.page_source
                )
            except:
                logger.debug(f"Could not find 'Searched Result' for code {code}, waiting for generic page load")
                # If specific text not found, wait for generic page load completion
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
            
            logger.debug(f"Results loaded for code {code} in {time.time() - results_start:.2f}s")
        except TimeoutException:
            logger.warning(f"Timeout waiting for results for code {code}, proceeding anyway")
        
        # Wait a little more to ensure all dynamic content is loaded
        time.sleep(2)
        
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
        from bs4 import BeautifulSoup
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
                            
                            # Check label-value pairs
                            if len(cells) >= 2:
                                # Check if first cell contains road name label
                                if 'Road Name' in cells[0].text and not road_name:
                                    road_name = cells[1].text.strip()
                                
                                # Check if first cell contains bus description label
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
            'timestamp': datetime.datetime.now().isoformat(),
            'processing_time': time.time() - start_time
        }
        
        logger.debug(f"Completed scraping for code {code} in {result['processing_time']:.2f}s: success={result['success']}")
        return result
    
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error scraping bus stop {code}: {error_message}")
        logger.debug(traceback.format_exc())
        
        return {
            'code': code, 
            'success': False, 
            'error': error_message, 
            'timestamp': datetime.datetime.now().isoformat(),
            'processing_time': time.time() - start_time if 'start_time' in locals() else 0
        }
    
    finally:
        # Return driver to pool
        if driver:
            try:
                driver_pool.return_driver(driver)
                logger.debug(f"Returned driver to pool for code {code}")
            except Exception as e:
                logger.warning(f"Failed to return driver to pool for code {code}: {str(e)}")
                # Try to close driver directly
                try:
                    driver.quit()
                except:
                    pass
                
def merge_bus_stop_data(lta_df, simplygo_df, output_file=None):
    """
    Merge LTA DataMall and SimplyGo data
    
    Args:
        lta_df: DataFrame with LTA DataMall data
        simplygo_df: DataFrame with SimplyGo data
        output_file: Path to save merged CSV output
        
    Returns:
        DataFrame with merged data
    """
    with PerformanceTimer("Data merging"):
        logger.info("Starting data merge process...")
        
        if lta_df.empty:
            logger.error("LTA DataFrame is empty")
            return pd.DataFrame()
            
        if simplygo_df.empty:
            logger.warning("SimplyGo DataFrame is empty, returning LTA data as is")
            return lta_df
        
        try:
            # Ensure 'code' columns are strings for proper merging
            lta_df['code'] = lta_df['code'].astype(str)
            simplygo_df['code'] = simplygo_df['code'].astype(str)
            
            # Create a copy of LTA DataFrame to preserve original data
            merged_df = lta_df.copy()
            
            # Create a column for corrected names
            merged_df['corrected_name'] = merged_df['name']
            
            # Prepare SimplyGo data - rename columns for clarity
            simplygo_prepared = simplygo_df.rename(columns={
                'bus_description': 'simplygo_name',
                'road_name': 'simplygo_street'
            })
            
            # Keep only necessary columns from SimplyGo
            simplygo_prepared = simplygo_prepared[['code', 'simplygo_name', 'simplygo_street', 'success']]
            
            # Filter out unsuccessful scrapes
            simplygo_valid = simplygo_prepared[simplygo_prepared['success'] == True].copy()
            
            # Drop records with empty SimplyGo names
            simplygo_valid = simplygo_valid[simplygo_valid['simplygo_name'].notna() & 
                                           (simplygo_valid['simplygo_name'] != '')]
            
            logger.info(f"Valid SimplyGo records for merging: {len(simplygo_valid)}")
            
            # Merge the dataframes
            merged_df = pd.merge(merged_df, simplygo_valid[['code', 'simplygo_name']], 
                                 on='code', how='left')
            
            # Apply correction rule: Use SimplyGo name if available
            mask = merged_df['simplygo_name'].notna()
            merged_df.loc[mask, 'corrected_name'] = merged_df.loc[mask, 'simplygo_name']
            
            # Count how many names were corrected
            corrected_count = mask.sum()
            logger.info(f"Corrected {corrected_count} bus stop names using SimplyGo data")
            
            # Create a flag column to indicate source of the name
            merged_df['name_source'] = 'LTA'
            merged_df.loc[mask, 'name_source'] = 'SimplyGo'
            
            # Clean up the final DataFrame
            final_df = merged_df[['code', 'name', 'corrected_name', 'name_source', 'street', 'lat', 'lon']]
            
            # Save to file if output_file is provided
            if output_file:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
                
                final_df.to_csv(output_file, index=False)
                logger.info(f"Saved merged data to {output_file}")
            
            return final_df
        
        except Exception as e:
            logger.error(f"Error merging data: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()  # Return empty DataFrame on error

def run_optimized_workflow(lta_api_key, lta_email=None, lta_password=None, workers=4, batch_size=20, limit=None):
    """
    Run the optimized workflow:
    1. Download from LTA DataMall
    2. Compare with previous data
    3. Scrape SimplyGo only for new/changed bus stops
    4. Merge data
    
    Args:
        lta_api_key: LTA DataMall API key
        lta_email: LTA DataMall email (optional)
        lta_password: LTA DataMall password (optional)
        workers: Number of parallel workers for SimplyGo scraping
        batch_size: Batch size for saving progress
        limit: Limit number of bus stops to process (for testing)
        
    Returns:
        Tuple of (lta_df, simplygo_df, merged_df, diff_df)
    """
    with PerformanceTimer("Optimized workflow"):
        # Generate timestamp for filenames
        current_date = datetime.datetime.now().strftime("%d%m%Y")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('output', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Step 1: Download from LTA DataMall
        lta_output_file = f"data/LTA_bus_stops_{current_date}.csv"
        lta_df = download_lta_datamall(lta_api_key, lta_email, lta_password, lta_output_file)
        
        if lta_df.empty:
            logger.error("Failed to download LTA DataMall data. Aborting workflow.")
            return None, None, None, None
        
        # Step 2: Find previous LTA data file and compare
        # Find file from previous date (not current date)
        pattern = os.path.join('data', "LTA_bus_stops_*.csv")
        all_files = glob.glob(pattern)
        previous_files = []
        for file in all_files:
            filename = os.path.basename(file)
            date_match = re.search(r'LTA_bus_stops_(\d{8})\.csv', filename)
            if date_match and date_match.group(1) != current_date:
                previous_files.append(file)
        
        previous_lta_file = None
        if previous_files:
            previous_files.sort(reverse=True)  # Sort newest first
            previous_lta_file = previous_files[0]
            logger.info(f"Found previous LTA DataMall file: {previous_lta_file}")
        else:
            logger.info("No previous LTA DataMall files found (from different dates)")
        
        # Compare with previous file to find differences
        if not previous_lta_file:
            logger.info("No previous data file found. Processing all bus stops.")
            diff_df = lta_df.copy()
            diff_df['change_type'] = 'new'  # Mark all as new
            
            # Create empty difference file for completeness
            diff_file = f"data/LTA_difference_{current_date}.csv"
            diff_df.to_csv(diff_file, index=False)
        else:
            # Extract date from previous file name for diff file naming
            prev_date = os.path.basename(previous_lta_file).replace("LTA_bus_stops_", "").replace(".csv", "")
            
            # Compare data and save differences
            diff_file = f"data/LTA_difference_{prev_date}-{current_date}.csv"
            diff_df = compare_lta_data(lta_df, previous_lta_file, diff_file)
        
        # Check if there are differences to process
        if diff_df.empty:
            logger.info("No differences found between current and previous data.")
            logger.info("Skipping SimplyGo scraping and using existing LTA data.")
            
            # Copy the current LTA file to the standard output file
            merged_df = lta_df.copy()
            merged_df['corrected_name'] = merged_df['name']
            merged_df['name_source'] = 'LTA'
            
            # Save as merged output for consistency
            merged_output_file = f"data/lta_correction_{timestamp}.csv"
            merged_df.to_csv(merged_output_file, index=False)
            
            # Make a copy with consistent filename
            consistent_file = "data/lta_correction.csv"
            merged_df.to_csv(consistent_file, index=False)
            
            return lta_df, pd.DataFrame(), merged_df, diff_df
        
        # Step 3: Extract and scrape only the changed/new bus codes from SimplyGo
        # THIS IS THE KEY PART - Only process the bus codes that actually changed
        logger.info(f"Processing {len(diff_df)} bus codes from difference file")
        bus_codes = diff_df['code'].astype(str).tolist()
        
        # Log detail of the changed/new bus codes
        logger.info(f"Bus codes to be processed: {', '.join(bus_codes[:20])}{', ... more' if len(bus_codes) > 20 else ''}")
        
        # Apply limit if specified (for testing)
        if limit and limit > 0:
            logger.info(f"Limiting to {limit} bus codes for processing")
            bus_codes = bus_codes[:limit]
        
        # Scrape SimplyGo data only for the differences
        simplygo_output_file = f"output/simplygo_bus_stops_{timestamp}.csv"
        
        try:
            # Try to scrape SimplyGo data
            simplygo_results = scrape_parallel(bus_codes, n_workers=workers, batch_size=batch_size)
            
            if not simplygo_results:
                logger.warning("No results from SimplyGo scraping. Using LTA data only.")
                simplygo_df = pd.DataFrame()
            else:
                simplygo_df = pd.DataFrame(simplygo_results)
        except Exception as e:
            # Handle any exception during scraping
            logger.error(f"Error during SimplyGo scraping: {str(e)}")
            logger.error(traceback.format_exc())
            logger.warning("Proceeding with LTA data only due to scraping error.")
            simplygo_df = pd.DataFrame()
        
        # Step 4: Merge LTA and SimplyGo data for differences
        diff_correction_file = None
        if previous_lta_file:
            prev_date = os.path.basename(previous_lta_file).replace("LTA_bus_stops_", "").replace(".csv", "")
            diff_correction_file = f"data/LTA_difference_correction_{prev_date}-{current_date}.csv"
        else:
            diff_correction_file = f"data/LTA_difference_correction_{current_date}.csv"
        
        # If we have SimplyGo data, merge it with diff data
        if not simplygo_df.empty:
            # Merge data just for the differences
            try:
                diff_merged_df = merge_bus_stop_data(diff_df, simplygo_df, diff_correction_file)
            except Exception as e:
                logger.error(f"Error during difference merging: {str(e)}")
                logger.error(traceback.format_exc())
                diff_merged_df = None
        else:
            # No SimplyGo data, just use diff data as is
            diff_merged_df = diff_df.copy()
            if 'corrected_name' not in diff_merged_df.columns:
                diff_merged_df['corrected_name'] = diff_merged_df['name']
            if 'name_source' not in diff_merged_df.columns:
                diff_merged_df['name_source'] = 'LTA'
            
            # Save the unmerged diff data for reference
            if diff_correction_file:
                diff_merged_df.to_csv(diff_correction_file, index=False)
                logger.info(f"Saved unmerged difference data to {diff_correction_file}")
        
        # Step 5: Apply corrections to the full dataset
        merged_output_file = f"data/lta_correction_{timestamp}.csv"
        
        try:
            # Create copy of full LTA dataset
            full_merged_df = lta_df.copy()
            
            # Add required columns if they don't exist
            if 'corrected_name' not in full_merged_df.columns:
                full_merged_df['corrected_name'] = full_merged_df['name']
            if 'name_source' not in full_merged_df.columns:
                full_merged_df['name_source'] = 'LTA'
            
            # Update with corrections from the differences if available
            if diff_merged_df is not None and not diff_merged_df.empty:
                # Ensure code columns are string type
                full_merged_df['code'] = full_merged_df['code'].astype(str)
                diff_merged_df['code'] = diff_merged_df['code'].astype(str)
                
                # Only update if we have the required columns
                if 'corrected_name' in diff_merged_df.columns and 'name_source' in diff_merged_df.columns:
                    for _, row in diff_merged_df.iterrows():
                        # Find matching record in full dataset
                        idx = full_merged_df[full_merged_df['code'] == row['code']].index
                        
                        if not idx.empty:
                            # Update the record
                            full_merged_df.loc[idx, 'corrected_name'] = row['corrected_name']
                            full_merged_df.loc[idx, 'name_source'] = row['name_source']
            
            # Save merged dataset
            full_merged_df.to_csv(merged_output_file, index=False)
            logger.info(f"Saved merged dataset to {merged_output_file}")
            
            # Make a copy with a consistent filename for automated access
            consistent_file = "data/lta_correction.csv"
            full_merged_df.to_csv(consistent_file, index=False)
            logger.info(f"Saved a copy to {consistent_file}")
            
            merged_df = full_merged_df
        except Exception as e:
            logger.error(f"Error applying corrections to full dataset: {str(e)}")
            logger.error(traceback.format_exc())
            
            # If merging failed, use LTA data as is
            merged_df = lta_df.copy()
            if 'corrected_name' not in merged_df.columns:
                merged_df['corrected_name'] = merged_df['name']
            if 'name_source' not in merged_df.columns:
                merged_df['name_source'] = 'LTA'
                
            merged_df.to_csv(merged_output_file, index=False)
            logger.warning(f"Using LTA data as is due to merging error, saved to {merged_output_file}")
            
            # Make a copy with a consistent filename
            consistent_file = "data/lta_correction.csv"
            merged_df.to_csv(consistent_file, index=False)
            logger.info(f"Saved a copy to {consistent_file}")
        
        return lta_df, simplygo_df, merged_df, diff_df

def main():
    """Main function to run the workflow"""
    # Configure logging
    setup_logging(logging.INFO)
    
    # Setup command line parser
    parser = argparse.ArgumentParser(description='Bus Stop Data Collector and Merger (Optimized)')
    parser.add_argument('--lta-api-key', type=str, required=True, help='LTA DataMall API Key')
    parser.add_argument('--lta-email', type=str, help='LTA DataMall email (optional)')
    parser.add_argument('--lta-password', type=str, help='LTA DataMall password (optional)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers for SimplyGo scraping')
    parser.add_argument('--batch-size', type=int, default=20, help='Batch size for saving progress')
    parser.add_argument('--limit', type=int, help='Limit number of bus stops to process (for testing)')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (save HTML and screenshots)')
    
    args = parser.parse_args()
    
    # Configure log level
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)
    
    # Print system info
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Starting optimized workflow with {args.workers} workers")
    
    # Run the optimized workflow
    lta_df, simplygo_df, merged_df, diff_df = run_optimized_workflow(
        args.lta_api_key, args.lta_email, args.lta_password, 
        args.workers, args.batch_size, args.limit
    )
    
    if merged_df is not None and not merged_df.empty:
        logger.info("Workflow completed successfully!")
        
        # Show summary
        total_records = len(merged_df)
        corrected_records = (merged_df['name_source'] == 'SimplyGo').sum()
        
        logger.info(f"Total bus stops: {total_records}")
        logger.info(f"Corrected names: {corrected_records} ({corrected_records/total_records*100:.1f}%)")
        
        # If we have differences, show statistics
        if diff_df is not None and not diff_df.empty:
            new_stops = diff_df[diff_df['change_type'] == 'new'].shape[0]
            changed_stops = diff_df[diff_df['change_type'] == 'changed'].shape[0]
            
            logger.info(f"Changes from previous dataset: {len(diff_df)} total")
            logger.info(f"  - New bus stops: {new_stops}")
            logger.info(f"  - Modified bus stops: {changed_stops}")
        
        # Sample corrected records
        if corrected_records > 0:
            sample_corrected = merged_df[merged_df['name_source'] == 'SimplyGo'].head(5)
            logger.info("Sample corrected records:")
            for _, row in sample_corrected.iterrows():
                logger.info(f"Code: {row['code']} | LTA: '{row['name']}' -> SimplyGo: '{row['corrected_name']}'")
    else:
        logger.error("Workflow completed with errors.")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Script execution finished")