#!/usr/bin/env python3
"""
Quick test script to verify code normalization fix
Run this first to see if the fix works correctly
"""

import pandas as pd
import os

def normalize_bus_code(code):
    """Normalize bus code to 5-digit format"""
    try:
        code_str = str(code).strip()
        if code_str == 'nan' or code_str == '' or code_str == 'None':
            return None
        code_int = int(float(code_str))
        return f"{code_int:05d}"
    except (ValueError, TypeError):
        print(f"Warning: Could not normalize code: {code}")
        return None

def test_normalization():
    """Test the normalization with your existing files"""
    
    print("=" * 60)
    print("TESTING CODE NORMALIZATION FIX")
    print("=" * 60)
    
    # Check if files exist
    old_file = "data/LTA_bus_stops_16052025.csv"
    new_file = "data/LTA_bus_stops_27052025.csv"
    
    if not os.path.exists(old_file):
        print(f"❌ File not found: {old_file}")
        return
    
    if not os.path.exists(new_file):
        print(f"❌ File not found: {new_file}")
        return
    
    # Load both files
    print("📁 Loading files...")
    old_df = pd.read_csv(old_file)
    new_df = pd.read_csv(new_file)
    
    print(f"Old file: {len(old_df)} records")
    print(f"New file: {len(new_df)} records")
    
    # Show sample raw codes
    print(f"\n📋 SAMPLE RAW CODES:")
    print(f"Old file codes: {old_df['code'].head(10).tolist()}")
    print(f"New file codes: {new_df['code'].head(10).tolist()}")
    
    # Normalize codes
    print(f"\n🔧 Applying normalization...")
    old_df['code_normalized'] = old_df['code'].apply(normalize_bus_code)
    new_df['code_normalized'] = new_df['code'].apply(normalize_bus_code)
    
    # Remove failed normalizations
    old_df = old_df[old_df['code_normalized'].notna()]
    new_df = new_df[new_df['code_normalized'].notna()]
    
    print(f"After cleaning: Old={len(old_df)}, New={len(new_df)}")
    
    # Show sample normalized codes
    print(f"\n📋 SAMPLE NORMALIZED CODES:")
    print(f"Old file codes: {old_df['code_normalized'].head(10).tolist()}")
    print(f"New file codes: {new_df['code_normalized'].head(10).tolist()}")
    
    # Compare
    old_codes = set(old_df['code_normalized'])
    new_codes = set(new_df['code_normalized'])
    
    added_codes = new_codes - old_codes
    removed_codes = old_codes - new_codes
    common_codes = new_codes & old_codes
    
    print(f"\n📊 COMPARISON RESULTS:")
    print(f"Old dataset: {len(old_codes):,} unique codes")
    print(f"New dataset: {len(new_codes):,} unique codes")
    print(f"Common codes: {len(common_codes):,}")
    print(f"Added codes: {len(added_codes):,}")
    print(f"Removed codes: {len(removed_codes):,}")
    print(f"Net change: {len(new_codes) - len(old_codes):+,}")
    
    # Show samples if there are changes
    if added_codes:
        sample_added = list(added_codes)[:5]
        print(f"\nSample added codes: {sample_added}")
        
        # Show original format for these codes
        for code in sample_added:
            original = new_df[new_df['code_normalized'] == code]['code'].iloc[0]
            print(f"  {code} (original: {original})")
    
    if removed_codes:
        sample_removed = list(removed_codes)[:5]
        print(f"\nSample removed codes: {sample_removed}")
        
        # Show original format for these codes
        for code in sample_removed:
            original = old_df[old_df['code_normalized'] == code]['code'].iloc[0]
            print(f"  {code} (original: {original})")
    
    # Check for name changes in common codes
    if common_codes:
        print(f"\n🔍 Checking name changes in common codes...")
        old_names = dict(zip(old_df['code_normalized'], old_df['name']))
        new_names = dict(zip(new_df['code_normalized'], new_df['name']))
        
        name_changes = 0
        sample_name_changes = []
        
        for code in list(common_codes)[:100]:  # Check first 100
            old_name = old_names.get(code, '').strip()
            new_name = new_names.get(code, '').strip()
            
            if old_name != new_name:
                name_changes += 1
                if len(sample_name_changes) < 3:
                    sample_name_changes.append((code, old_name, new_name))
        
        print(f"Name changes detected: {name_changes}")
        if sample_name_changes:
            print("Sample name changes:")
            for code, old_name, new_name in sample_name_changes:
                print(f"  {code}: '{old_name}' -> '{new_name}'")
    
    print(f"\n" + "=" * 60)
    if len(added_codes) + len(removed_codes) == 0:
        print("✅ SUCCESS: No spurious changes detected after normalization!")
        print("The code normalization fix works correctly.")
    else:
        print("⚠️  Still detecting changes - may be legitimate or need further investigation")
    print("=" * 60)

if __name__ == "__main__":
    test_normalization()