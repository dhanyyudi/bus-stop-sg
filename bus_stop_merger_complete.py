#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Complete Bus Stop Data Collector and Merger

This script performs comprehensive data comparison and correction:
1. Download ORIGINAL LTA DataMall data
2. Compare comprehensively with previous LTA data:
   - Check quantity changes (total bus stops)
   - Check code changes (new/removed bus stops)
   - Check name changes (for same codes)
3. Scrape SimplyGo only for changed bus stops
4. Apply corrections to ORIGINAL LTA data (not pre-corrected data)

Key Features:
- Uses ORIGINAL LTA data for all comparisons and final output
- Comprehensive change detection (quantity + codes + names)
- Efficient scraping (only changed bus stops)
- Detailed logging and statistics

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
import glob
import subprocess
import re

# Import the existing scraper
try:
    from parallelized_simplygo_scraper import scrape_parallel
except ImportError:
    print("WARNING: Could not import scrape_parallel. Make sure parallelized_simplygo_scraper.py is available.")
    print("Continuing without SimplyGo scraping functionality...")
    scrape_parallel = None

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
    Returns ORIGINAL/RAW data without any corrections
    
    Args:
        api_key: LTA DataMall API key
        email: LTA DataMall email (optional)
        password: LTA DataMall password (optional)
        output_file: Path to save CSV output
        
    Returns:
        DataFrame with ORIGINAL LTA DataMall data
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
                
                # Add delay to respect API rate limits
                if i > 0:
                    time.sleep(1)
                
                # Make the API request
                try:
                    with PerformanceTimer(f"API request (skip={i})"):
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
                        backoff_time = retry_count * 5
                        logger.info(f"Retrying in {backoff_time} seconds... (attempt {retry_count}/{max_retries})")
                        time.sleep(backoff_time)
                        
                        try:
                            response = requests.request("GET", url, headers=headers, timeout=30)
                            
                            if response.status_code == 200:
                                logger.info("Retry successful")
                                data = response.json()
                                if len(data['value']) == 0:
                                    logger.info("No more records to fetch from API")
                                    i = None
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
                                    
                                    i += 500
                                    break
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
            os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} ORIGINAL records to {output_file}")
        
        return df

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

def log_detailed_comparison_statistics(new_df, old_file):
    """
    Log comprehensive comparison statistics between datasets
    Analyze: quantity changes, code changes, and name changes
    
    Args:
        new_df: DataFrame with new LTA data (original, not corrected)
        old_file: Path to the previous LTA data file (original, not corrected)
    """
    try:
        # Load previous data
        old_df = pd.read_csv(old_file)
        
        # Clean data for comparison
        old_df['code'] = old_df['code'].astype(str).str.strip()
        new_df['code'] = new_df['code'].astype(str).str.strip()
        old_df['name'] = old_df['name'].astype(str).str.strip()
        new_df['name'] = new_df['name'].astype(str).str.strip()
        
        # Get sets of bus codes
        old_codes = set(old_df['code'])
        new_codes = set(new_df['code'])
        
        # Calculate code differences
        added_codes = new_codes - old_codes  # New bus stops
        removed_codes = old_codes - new_codes  # Removed bus stops
        common_codes = new_codes & old_codes  # Same bus stops
        
        # Check name changes for common codes
        changed_name_codes = set()
        if common_codes:
            # Create dictionaries for easier comparison
            old_names = dict(zip(old_df['code'], old_df['name']))
            new_names = dict(zip(new_df['code'], new_df['name']))
            
            for code in common_codes:
                old_name = old_names.get(code, '')
                new_name = new_names.get(code, '')
                if old_name != new_name:
                    changed_name_codes.add(code)
        
        # Get file dates from filename for better logging
        old_filename = os.path.basename(old_file)
        old_date_match = re.search(r'(\d{8})', old_filename)
        old_date_str = old_date_match.group(1) if old_date_match else "unknown"
        
        current_date = datetime.datetime.now().strftime("%d%m%Y")
        
        # Calculate totals
        total_changes = len(added_codes) + len(removed_codes) + len(changed_name_codes)
        
        logger.info("=" * 90)
        logger.info("🔍 COMPREHENSIVE BUS STOP COMPARISON REPORT")
        logger.info("=" * 90)
        logger.info(f"📅 Previous dataset ({old_date_str}): {len(old_codes):,} bus stops")
        logger.info(f"📅 Current dataset  ({current_date}): {len(new_codes):,} bus stops")
        logger.info(f"📊 Net change in quantity: {len(new_codes) - len(old_codes):+,}")
        logger.info("-" * 90)
        
        # Detailed breakdown
        logger.info("🔢 CODE CHANGES:")
        logger.info(f"   🆕 New bus stop codes:     {len(added_codes):,}")
        logger.info(f"   🗑️  Removed bus stop codes: {len(removed_codes):,}")
        logger.info(f"   🔄 Unchanged codes:        {len(common_codes):,}")
        
        logger.info("📝 NAME CHANGES (for same codes):")
        logger.info(f"   ✏️  Bus stops with name changes: {len(changed_name_codes):,}")
        logger.info(f"   ✅ Bus stops with same names:   {len(common_codes) - len(changed_name_codes):,}")
        
        logger.info("-" * 90)
        logger.info(f"📈 TOTAL CHANGES DETECTED: {total_changes:,}")
        logger.info(f"   = {len(added_codes):,} new + {len(removed_codes):,} removed + {len(changed_name_codes):,} name changes")
        logger.info("-" * 90)
        
        # Show samples
        if added_codes:
            sample_new = list(added_codes)[:3]
            logger.info(f"🆕 Sample new codes: {', '.join(sample_new)}")
            if len(added_codes) > 3:
                logger.info(f"   ... and {len(added_codes) - 3} more new codes")
        
        if removed_codes:
            sample_removed = list(removed_codes)[:3]
            logger.info(f"🗑️  Sample removed codes: {', '.join(sample_removed)}")
            if len(removed_codes) > 3:
                logger.info(f"   ... and {len(removed_codes) - 3} more removed codes")
        
        if changed_name_codes:
            sample_changed = list(changed_name_codes)[:3]
            logger.info(f"✏️  Sample name changes:")
            old_names = dict(zip(old_df['code'], old_df['name']))
            new_names = dict(zip(new_df['code'], new_df['name']))
            
            for code in sample_changed:
                old_name = old_names.get(code, 'N/A')
                new_name = new_names.get(code, 'N/A')
                logger.info(f"     {code}: '{old_name}' → '{new_name}'")
            
            if len(changed_name_codes) > 3:
                logger.info(f"   ... and {len(changed_name_codes) - 3} more name changes")
        
        logger.info("=" * 90)
        
        # Decision logic
        if total_changes == 0:
            logger.info("🚫 NO CHANGES DETECTED - SimplyGo scraping will be SKIPPED")
            logger.info("   Current LTA data is identical to previous LTA data")
        else:
            logger.info(f"✅ {total_changes:,} CHANGES DETECTED - Will proceed with SimplyGo scraping")
            logger.info("   Will scrape SimplyGo for all changed bus stops to get corrected names")
        
        logger.info("=" * 90)
        
        return {
            'added_codes': added_codes,
            'removed_codes': removed_codes,
            'changed_name_codes': changed_name_codes,
            'total_changes': total_changes
        }
        
    except Exception as e:
        logger.error(f"Error generating detailed comparison statistics: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'added_codes': set(),
            'removed_codes': set(),
            'changed_name_codes': set(),
            'total_changes': 0
        }

def compare_lta_data_comprehensive(new_df, old_file, output_diff_file=None):
    """
    Comprehensive comparison of LTA data focusing on:
    1. Bus stop quantity changes
    2. Bus stop code changes (added/removed)
    3. Bus stop name changes (for same codes)
    
    Args:
        new_df: DataFrame with new LTA data (original, not corrected)
        old_file: Path to the previous LTA data file (original, not corrected)
        output_diff_file: Path to save changes CSV output
        
    Returns:
        DataFrame with ALL changed bus stops (new codes + removed codes + name changes)
    """
    with PerformanceTimer("Comprehensive LTA data comparison"):
        logger.info(f"Performing comprehensive comparison with previous data from {old_file}")
        
        try:
            # Load previous data
            old_df = pd.read_csv(old_file)
            
            # Clean data for comparison
            old_df['code'] = old_df['code'].astype(str).str.strip()
            new_df['code'] = new_df['code'].astype(str).str.strip()
            old_df['name'] = old_df['name'].astype(str).str.strip()
            new_df['name'] = new_df['name'].astype(str).str.strip()
            
            # Get detailed comparison statistics
            comparison_stats = log_detailed_comparison_statistics(new_df, old_file)
            
            # Extract the change sets
            added_codes = comparison_stats['added_codes']
            removed_codes = comparison_stats['removed_codes']
            changed_name_codes = comparison_stats['changed_name_codes']
            total_changes = comparison_stats['total_changes']
            
            # If no changes, return empty DataFrame
            if total_changes == 0:
                logger.info("No changes detected. No SimplyGo scraping needed.")
                return pd.DataFrame()
            
            # Collect all changes for processing
            changes_list = []
            
            # 1. Add new bus stops (from current data)
            if added_codes:
                new_stops = new_df[new_df['code'].isin(added_codes)].copy()
                new_stops['change_type'] = 'new'
                new_stops['change_reason'] = 'New bus stop code'
                changes_list.append(new_stops)
                logger.info(f"Added {len(new_stops)} new bus stops to processing list")
            
            # 2. Add removed bus stops (from old data, for reference)
            if removed_codes:
                removed_stops = old_df[old_df['code'].isin(removed_codes)].copy()
                removed_stops['change_type'] = 'removed'
                removed_stops['change_reason'] = 'Removed bus stop code'
                changes_list.append(removed_stops)
                logger.info(f"Added {len(removed_stops)} removed bus stops to processing list (for reference)")
            
            # 3. Add name-changed bus stops (from current data with latest names)
            if changed_name_codes:
                name_changed_stops = new_df[new_df['code'].isin(changed_name_codes)].copy()
                name_changed_stops['change_type'] = 'name_changed'
                name_changed_stops['change_reason'] = 'Bus stop name changed'
                
                # Add old names for reference
                old_names_dict = dict(zip(old_df['code'], old_df['name']))
                name_changed_stops['old_name'] = name_changed_stops['code'].map(old_names_dict)
                
                changes_list.append(name_changed_stops)
                logger.info(f"Added {len(name_changed_stops)} name-changed bus stops to processing list")
            
            # Combine all changes
            if changes_list:
                all_changes_df = pd.concat(changes_list, ignore_index=True)
                
                # Sort by change type and code for better organization
                sort_order = {'new': 1, 'name_changed': 2, 'removed': 3}
                all_changes_df['sort_order'] = all_changes_df['change_type'].map(sort_order)
                all_changes_df = all_changes_df.sort_values(['sort_order', 'code']).drop('sort_order', axis=1)
                
                logger.info(f"📋 Total changes to process: {len(all_changes_df)}")
                logger.info(f"   - Will scrape SimplyGo for: {len(all_changes_df[all_changes_df['change_type'].isin(['new', 'name_changed'])])} bus stops")
                logger.info(f"   - Removed bus stops (reference only): {len(all_changes_df[all_changes_df['change_type'] == 'removed'])}")
            else:
                all_changes_df = pd.DataFrame()
            
            # Save to file if output_diff_file is provided
            if output_diff_file and not all_changes_df.empty:
                os.makedirs(os.path.dirname(output_diff_file) if os.path.dirname(output_diff_file) else '.', exist_ok=True)
                all_changes_df.to_csv(output_diff_file, index=False)
                logger.info(f"Saved {len(all_changes_df)} changes to {output_diff_file}")
            
            return all_changes_df
        
        except Exception as e:
            logger.error(f"Error in comprehensive LTA data comparison: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()

def filter_changes_for_scraping(changes_df):
    """
    Filter changes to get only bus stops that need SimplyGo scraping
    (excludes removed bus stops since they don't exist anymore)
    
    Args:
        changes_df: DataFrame with all changes
        
    Returns:
        List of bus codes that need SimplyGo scraping
    """
    if changes_df.empty:
        return []
    
    # Only scrape new and name-changed bus stops (not removed ones)
    scraping_mask = changes_df['change_type'].isin(['new', 'name_changed'])
    codes_to_scrape = changes_df[scraping_mask]['code'].astype(str).tolist()
    
    logger.info(f"🎯 Bus stops to scrape from SimplyGo: {len(codes_to_scrape)}")
    
    # Log breakdown
    if not changes_df.empty:
        new_count = len(changes_df[changes_df['change_type'] == 'new'])
        name_changed_count = len(changes_df[changes_df['change_type'] == 'name_changed'])
        removed_count = len(changes_df[changes_df['change_type'] == 'removed'])
        
        logger.info(f"   📊 Breakdown:")
        logger.info(f"      🆕 New bus stops: {new_count}")
        logger.info(f"      ✏️  Name changed: {name_changed_count}")
        logger.info(f"      🗑️  Removed (skip): {removed_count}")
    
    return codes_to_scrape

def run_enhanced_workflow(lta_api_key, lta_email=None, lta_password=None, workers=4, batch_size=20, limit=None):
    """
    Run the enhanced workflow with comprehensive comparison logic:
    1. Download ORIGINAL LTA DataMall data
    2. Compare comprehensively: quantity + codes + names
    3. Scrape SimplyGo for changed bus stops only
    4. Apply corrections to ORIGINAL LTA data
    
    Args:
        lta_api_key: LTA DataMall API key
        lta_email: LTA DataMall email (optional)
        lta_password: LTA DataMall password (optional)
        workers: Number of parallel workers for SimplyGo scraping
        batch_size: Batch size for saving progress
        limit: Limit number of bus stops to process (for testing)
        
    Returns:
        Tuple of (lta_original_df, simplygo_df, corrected_df, changes_df)
    """
    with PerformanceTimer("Enhanced comprehensive workflow"):
        # Generate timestamp for filenames
        current_date = datetime.datetime.now().strftime("%d%m%Y")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('output', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logger.info("🚀 STARTING ENHANCED BUS STOP DATA COLLECTION WORKFLOW")
        logger.info("=" * 80)
        
        # Step 1: Download ORIGINAL LTA DataMall data
        logger.info("📡 STEP 1: Downloading original LTA DataMall data...")
        lta_output_file = f"data/LTA_bus_stops_{current_date}.csv"
        lta_original_df = download_lta_datamall(lta_api_key, lta_email, lta_password, lta_output_file)
        
        if lta_original_df.empty:
            logger.error("❌ Failed to download LTA DataMall data. Aborting workflow.")
            return None, None, None, None
        
        logger.info(f"✅ Downloaded {len(lta_original_df):,} bus stops from LTA DataMall")
        
        # Step 2: Find previous LTA data file and perform comprehensive comparison
        logger.info("🔍 STEP 2: Performing comprehensive data comparison...")
        previous_lta_file = get_previous_lta_file(current_date)
        
        if not previous_lta_file:
            logger.info("=" * 80)
            logger.info("🆕 FIRST RUN DETECTED - No previous data available")
            logger.info("=" * 80)
            logger.info("This appears to be the first run. All bus stops will be treated as NEW.")
            logger.info("Will proceed with SimplyGo scraping for all bus stops...")
            
            # Treat all bus stops as new
            changes_df = lta_original_df.copy()
            changes_df['change_type'] = 'new'
            changes_df['change_reason'] = 'First run - all new'
            
            # Save changes file
            changes_file = f"data/LTA_changes_{current_date}.csv"
            changes_df.to_csv(changes_file, index=False)
            
        else:
            # Extract date from previous file name
            prev_date = os.path.basename(previous_lta_file).replace("LTA_bus_stops_", "").replace(".csv", "")
            
            # Perform comprehensive comparison
            changes_file = f"data/LTA_changes_{prev_date}-{current_date}.csv"
            changes_df = compare_lta_data_comprehensive(lta_original_df, previous_lta_file, changes_file)
        
        # Step 3: Determine bus stops to scrape from SimplyGo
        logger.info("🎯 STEP 3: Determining bus stops for SimplyGo scraping...")
        
        if changes_df.empty:
            logger.info("=" * 70)
            logger.info("🚫 NO CHANGES DETECTED")
            logger.info("=" * 70)
            logger.info("Current LTA data is identical to previous LTA data.")
            logger.info("✅ Skipping SimplyGo scraping - no changes to process.")
            logger.info("📄 Using original LTA data as final result.")
            
            # Create final output with original LTA data
            corrected_df = lta_original_df.copy()
            corrected_df['corrected_name'] = corrected_df['name']
            corrected_df['name_source'] = 'LTA'
            
            # Save outputs
            corrected_output_file = f"data/lta_correction_{timestamp}.csv"
            corrected_df.to_csv(corrected_output_file, index=False)
            
            consistent_file = "data/lta_correction.csv"
            corrected_df.to_csv(consistent_file, index=False)
            
            logger.info(f"💾 Saved LTA-only data to {corrected_output_file}")
            logger.info(f"💾 Saved consistent copy to {consistent_file}")
            
            # Return with empty SimplyGo DataFrame
            simplygo_df = pd.DataFrame()
            return lta_original_df, simplygo_df, corrected_df, changes_df
        
        # Filter changes to get codes for scraping
        bus_codes_to_scrape = filter_changes_for_scraping(changes_df)
        
        if not bus_codes_to_scrape:
            logger.info("🚫 No bus stops need SimplyGo scraping (only removed bus stops detected)")
            logger.info("📄 Using original LTA data as final result.")
            
            # Create final output with original LTA data
            corrected_df = lta_original_df.copy()
            corrected_df['corrected_name'] = corrected_df['name']
            corrected_df['name_source'] = 'LTA'
            
            # Save outputs
            corrected_output_file = f"data/lta_correction_{timestamp}.csv"
            corrected_df.to_csv(corrected_output_file, index=False)
            
            consistent_file = "data/lta_correction.csv"
            corrected_df.to_csv(consistent_file, index=False)
            
            simplygo_df = pd.DataFrame()
            return lta_original_df, simplygo_df, corrected_df, changes_df
        
        # Apply limit if specified (for testing)
        if limit and limit > 0:
            logger.info(f"🧪 TEST MODE: Limiting to {limit} bus codes for testing")
            bus_codes_to_scrape = bus_codes_to_scrape[:limit]
        
        logger.info("=" * 70)
        logger.info(f"🎯 CHANGES DETECTED: {len(changes_df)} total changes")
        logger.info(f"🕷️  WILL SCRAPE: {len(bus_codes_to_scrape)} bus stops from SimplyGo")
        logger.info("=" * 70)
        
        # Log sample codes to scrape
        if len(bus_codes_to_scrape) <= 10:
            logger.info(f"Bus codes to scrape: {', '.join(bus_codes_to_scrape)}")
        else:
            logger.info(f"Sample codes to scrape: {', '.join(bus_codes_to_scrape[:10])}, ... and {len(bus_codes_to_scrape)-10} more")
        
        # Step 4: Scrape SimplyGo data for changed bus stops
        logger.info("🕷️  STEP 4: Scraping SimplyGo for changed bus stops...")
        
        if scrape_parallel is None:
            logger.warning("⚠️  SimplyGo scraping not available. Using original LTA data only.")
            simplygo_df = pd.DataFrame()
        else:
            try:
                # Scrape SimplyGo data
                simplygo_results = scrape_parallel(bus_codes_to_scrape, n_workers=workers, batch_size=batch_size)
                
                if not simplygo_results:
                    logger.warning("⚠️  No results from SimplyGo scraping. Using original LTA data only.")
                    simplygo_df = pd.DataFrame()
                else:
                    simplygo_df = pd.DataFrame(simplygo_results)
                    logger.info(f"✅ SimplyGo scraping completed: {len(simplygo_df)} results")
            except Exception as e:
                logger.error(f"❌ Error during SimplyGo scraping: {str(e)}")
                logger.error(traceback.format_exc())
                logger.warning("⚠️  Proceeding with original LTA data only due to scraping error.")
                simplygo_df = pd.DataFrame()
        
        # Step 5: Apply corrections to ORIGINAL LTA data
        logger.info("🔧 STEP 5: Applying corrections to original LTA data...")
        corrected_output_file = f"data/lta_correction_{timestamp}.csv"
        
        try:
            # Start with original LTA dataset (IMPORTANT: Use original, not corrected data)
            corrected_df = lta_original_df.copy()
            
            # Add correction columns
            corrected_df['corrected_name'] = corrected_df['name']  # Start with original names
            corrected_df['name_source'] = 'LTA'  # Default source is LTA
            
            # Apply corrections from SimplyGo data
            corrections_applied = 0
            if not simplygo_df.empty:
                # Ensure code columns are string type
                corrected_df['code'] = corrected_df['code'].astype(str)
                simplygo_df['code'] = simplygo_df['code'].astype(str)
                
                # Prepare SimplyGo data
                simplygo_prepared = simplygo_df.rename(columns={
                    'bus_description': 'simplygo_name'
                })
                
                # Filter successful scrapes with valid names
                simplygo_valid = simplygo_prepared[
                    (simplygo_prepared['success'] == True) & 
                    (simplygo_prepared['simplygo_name'].notna()) & 
                    (simplygo_prepared['simplygo_name'] != '') &
                    (simplygo_prepared['simplygo_name'].str.strip() != '')
                ]
                
                logger.info(f"📊 Valid SimplyGo corrections available: {len(simplygo_valid)}")
                
                # Apply corrections to original LTA data
                for _, row in simplygo_valid.iterrows():
                    mask = corrected_df['code'] == row['code']
                    if mask.any():
                        old_name = corrected_df.loc[mask, 'name'].iloc[0]
                        new_name = row['simplygo_name']
                        
                        corrected_df.loc[mask, 'corrected_name'] = new_name
                        corrected_df.loc[mask, 'name_source'] = 'SimplyGo'
                        corrections_applied += 1
                        
                        logger.debug(f"✏️  Corrected {row['code']}: '{old_name}' → '{new_name}'")
            
            logger.info(f"✅ Applied {corrections_applied} corrections from SimplyGo to original LTA data")
            
            # Save corrected dataset
            corrected_df.to_csv(corrected_output_file, index=False)
            logger.info(f"💾 Saved corrected dataset to {corrected_output_file}")
            
            # Make consistent copy
            consistent_file = "data/lta_correction.csv"
            corrected_df.to_csv(consistent_file, index=False)
            logger.info(f"💾 Saved consistent copy to {consistent_file}")
            
        except Exception as e:
            logger.error(f"❌ Error applying corrections to original LTA data: {str(e)}")
            logger.error(traceback.format_exc())
            
            # If correction failed, use original LTA data as is
            corrected_df = lta_original_df.copy()
            corrected_df['corrected_name'] = corrected_df['name']
            corrected_df['name_source'] = 'LTA'
                
            corrected_df.to_csv(corrected_output_file, index=False)
            logger.warning(f"⚠️  Using original LTA data due to correction error, saved to {corrected_output_file}")
        
        logger.info("🏁 ENHANCED WORKFLOW COMPLETED")
        return lta_original_df, simplygo_df, corrected_df, changes_df

def main():
    """Main function to run the comprehensive workflow"""
    # Configure logging
    setup_logging(logging.INFO)
    
    # Setup command line parser
    parser = argparse.ArgumentParser(description='Complete Bus Stop Data Collector and Merger')
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
    logger.info(f"Starting COMPLETE workflow with {args.workers} workers")
    logger.info("🎯 This version uses ORIGINAL LTA data for all comparisons and corrections")
    
    # Run the enhanced workflow
    lta_original_df, simplygo_df, corrected_df, changes_df = run_enhanced_workflow(
        args.lta_api_key, args.lta_email, args.lta_password, 
        args.workers, args.batch_size, args.limit
    )
    
    if corrected_df is not None and not corrected_df.empty:
        logger.info("=" * 90)
        logger.info("✅ COMPLETE WORKFLOW FINISHED SUCCESSFULLY!")
        logger.info("=" * 90)
        
        # Show summary statistics
        total_records = len(corrected_df)
        corrected_records = (corrected_df['name_source'] == 'SimplyGo').sum()
        
        logger.info(f"📊 FINAL STATISTICS:")
        logger.info(f"   📍 Total bus stops: {total_records:,}")
        logger.info(f"   🔄 SimplyGo corrections applied: {corrected_records:,}")
        logger.info(f"   📈 Correction rate: {corrected_records/total_records*100:.1f}%")
        
        # Show change breakdown if we have changes
        if changes_df is not None and not changes_df.empty:
            new_stops = len(changes_df[changes_df['change_type'] == 'new']) if 'change_type' in changes_df.columns else 0
            name_changes = len(changes_df[changes_df['change_type'] == 'name_changed']) if 'change_type' in changes_df.columns else 0
            removed_stops = len(changes_df[changes_df['change_type'] == 'removed']) if 'change_type' in changes_df.columns else 0
            
            logger.info(f"📈 CHANGE BREAKDOWN:")
            logger.info(f"   🆕 New bus stops: {new_stops:,}")
            logger.info(f"   ✏️  Name changes: {name_changes:,}")
            logger.info(f"   🗑️  Removed stops: {removed_stops:,}")
            logger.info(f"   📊 Total changes: {len(changes_df):,}")
        
        # Sample corrected records
        if corrected_records > 0:
            sample_corrected = corrected_df[corrected_df['name_source'] == 'SimplyGo'].head(3)
            logger.info("📝 SAMPLE CORRECTIONS:")
            for _, row in sample_corrected.iterrows():
                logger.info(f"   {row['code']}: '{row['name']}' → '{row['corrected_name']}'")
            
            if corrected_records > 3:
                logger.info(f"   ... and {corrected_records - 3} more corrections")
        
        # Show efficiency metrics
        if changes_df is not None and not changes_df.empty:
            scrape_candidates = len(changes_df[changes_df['change_type'].isin(['new', 'name_changed'])])
            if scrape_candidates > 0:
                scrape_efficiency = (corrected_records / scrape_candidates) * 100
                logger.info(f"🎯 EFFICIENCY METRICS:")
                logger.info(f"   Scraping success rate: {scrape_efficiency:.1f}%")
                logger.info(f"   Processing efficiency: Only processed {len(changes_df):,} changes instead of all {total_records:,} bus stops")
        
        logger.info("=" * 90)
        logger.info("🎉 All data saved and ready for use!")
        logger.info("📂 Main output: data/lta_correction.csv")
        logger.info("=" * 90)
    else:
        logger.error("❌ Complete workflow finished with errors.")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Script execution finished")