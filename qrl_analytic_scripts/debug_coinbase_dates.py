import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def debug_coinbase_dates():
    """Debug function to understand coinbase transaction dates."""
    try:
        print("🔍 Debugging coinbase transaction dates...")
        
        # 1. Check what dates have coinbase transactions in the raw transaction data
        print("\n📊 Checking raw transaction data for coinbase transactions...")
        cur.execute('''
            SELECT DISTINCT DATE(block_found_datetime) as transaction_date
            FROM public."qrl_blockchain_transactions"
            WHERE "transaction_type" = 'coinbase'
            ORDER BY transaction_date ASC
        ''')
        result = cur.fetchall()
        raw_coinbase_dates = {row[0] for row in result}
        
        print(f"📊 Found {len(raw_coinbase_dates)} dates with coinbase transactions in raw data")
        
        if raw_coinbase_dates:
            print(f"📅 First coinbase date in raw data: {min(raw_coinbase_dates)}")
            print(f"📅 Last coinbase date in raw data: {max(raw_coinbase_dates)}")
        
        # 2. Check what dates have coinbase transactions in the aggregated data
        print("\n📊 Checking aggregated data for coinbase transactions...")
        cur.execute('''
            SELECT DISTINCT DATE("date") as coinbase_date
            FROM public."qrl_aggregated_transaction_data" 
            WHERE "transaction_type" = 'coinbase'
            ORDER BY coinbase_date ASC
        ''')
        result = cur.fetchall()
        aggregated_coinbase_dates = {row[0] for row in result}
        
        print(f"📊 Found {len(aggregated_coinbase_dates)} dates with coinbase transactions in aggregated data")
        
        if aggregated_coinbase_dates:
            print(f"📅 First coinbase date in aggregated data: {min(aggregated_coinbase_dates)}")
            print(f"📅 Last coinbase date in aggregated data: {max(aggregated_coinbase_dates)}")
        
        # 3. Show sample dates from both sets to compare
        print("\n📊 Sample comparison of dates:")
        raw_sample = sorted(list(raw_coinbase_dates))[:5]
        agg_sample = sorted(list(aggregated_coinbase_dates))[:5]
        
        print("Raw data sample dates:")
        for i, date in enumerate(raw_sample):
            print(f"  {i+1}. {date} (type: {type(date)})")
            
        print("Aggregated data sample dates:")
        for i, date in enumerate(agg_sample):
            print(f"  {i+1}. {date} (type: {type(date)})")
        
        # 4. Check if the sets are actually equal
        print(f"\n📊 Are the date sets equal? {raw_coinbase_dates == aggregated_coinbase_dates}")
        
        # 5. Show what's different
        if raw_coinbase_dates != aggregated_coinbase_dates:
            only_in_raw = raw_coinbase_dates - aggregated_coinbase_dates
            only_in_agg = aggregated_coinbase_dates - raw_coinbase_dates
            
            print(f"📊 Dates only in raw data: {len(only_in_raw)}")
            if only_in_raw:
                print("  Sample:", list(only_in_raw)[:5])
                
            print(f"📊 Dates only in aggregated data: {len(only_in_agg)}")
            if only_in_agg:
                print("  Sample:", list(only_in_agg)[:5])
        
        # 6. Calculate missing dates based on raw data (this should be the correct approach)
        print("\n📊 Calculating missing dates based on raw transaction data...")
        today = datetime.utcnow().date()
        
        # Only consider dates that actually have coinbase transactions in raw data
        # Don't assume every day from genesis to today should have coinbase transactions
        missing_dates = sorted(raw_coinbase_dates - aggregated_coinbase_dates)
        
        print(f"📊 Missing coinbase days (raw data vs aggregated): {len(missing_dates)}")
        
        if missing_dates:
            print(f"📅 First missing date: {missing_dates[0]}")
            print(f"📅 Last missing date: {missing_dates[-1]}")
            
            # Show some sample missing dates
            print("📅 Sample missing dates:")
            for i, date in enumerate(missing_dates[:10]):
                print(f"  {i+1}. {date}")
            if len(missing_dates) > 10:
                print(f"  ... and {len(missing_dates) - 10} more")
        
        return len(missing_dates), len(raw_coinbase_dates), len(aggregated_coinbase_dates)
        
    except Exception as e:
        print(f"❌ Error in debug: {e}")
        raise

if __name__ == "__main__":
    debug_coinbase_dates() 