import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def debug_missing_dates_detailed():
    """Detailed debug to find what dates are actually missing in aggregated data."""
    try:
        print("🔍 Detailed analysis of missing dates in aggregated transaction table...")
        
        # 1. Check what transaction types exist in raw data
        print("\n📊 Checking transaction types in raw data...")
        cur.execute('''
            SELECT DISTINCT "transaction_type", COUNT(*) as count
            FROM public."qrl_blockchain_transactions"
            GROUP BY "transaction_type"
            ORDER BY count DESC
        ''')
        result = cur.fetchall()
        raw_transaction_types = {row[0]: row[1] for row in result}
        
        print("Transaction types in raw data:")
        for tx_type, count in raw_transaction_types.items():
            print(f"  • {tx_type}: {count:,} transactions")
        
        # 2. Check what transaction types exist in aggregated data
        print("\n📊 Checking transaction types in aggregated data...")
        cur.execute('''
            SELECT DISTINCT "transaction_type", COUNT(*) as count
            FROM public."qrl_aggregated_transaction_data"
            GROUP BY "transaction_type"
            ORDER BY count DESC
        ''')
        result = cur.fetchall()
        aggregated_transaction_types = {row[0]: row[1] for row in result}
        
        print("Transaction types in aggregated data:")
        for tx_type, count in aggregated_transaction_types.items():
            print(f"  • {tx_type}: {count:,} records")
        
        # 3. Check date ranges for each transaction type
        print("\n📊 Checking date ranges for each transaction type...")
        
        for tx_type in raw_transaction_types.keys():
            print(f"\n📅 Transaction type: {tx_type}")
            
            # Raw data date range
            cur.execute('''
                SELECT MIN(DATE(block_found_datetime)) as first_date, 
                       MAX(DATE(block_found_datetime)) as last_date,
                       COUNT(DISTINCT DATE(block_found_datetime)) as unique_dates
                FROM public."qrl_blockchain_transactions"
                WHERE "transaction_type" = %s
            ''', (tx_type,))
            raw_result = cur.fetchone()
            
            if raw_result[0]:  # If there's data
                print(f"  Raw data: {raw_result[0]} to {raw_result[1]} ({raw_result[2]} unique dates)")
                
                # Aggregated data date range
                cur.execute('''
                    SELECT MIN(DATE("date")) as first_date, 
                           MAX(DATE("date")) as last_date,
                           COUNT(DISTINCT DATE("date")) as unique_dates
                    FROM public."qrl_aggregated_transaction_data"
                    WHERE "transaction_type" = %s
                ''', (tx_type,))
                agg_result = cur.fetchone()
                
                if agg_result[0]:  # If there's aggregated data
                    print(f"  Aggregated: {agg_result[0]} to {agg_result[1]} ({agg_result[2]} unique dates)")
                    
                    # Calculate missing dates
                    missing_count = raw_result[2] - agg_result[2]
                    if missing_count > 0:
                        print(f"  ⚠️ Missing {missing_count} dates!")
                        
                        # Find specific missing dates
                        cur.execute('''
                            SELECT DISTINCT DATE(block_found_datetime) as raw_date
                            FROM public."qrl_blockchain_transactions"
                            WHERE "transaction_type" = %s
                            ORDER BY raw_date
                        ''', (tx_type,))
                        raw_dates = {row[0] for row in cur.fetchall()}
                        
                        cur.execute('''
                            SELECT DISTINCT DATE("date") as agg_date
                            FROM public."qrl_aggregated_transaction_data"
                            WHERE "transaction_type" = %s
                            ORDER BY agg_date
                        ''', (tx_type,))
                        agg_dates = {row[0] for row in cur.fetchall()}
                        
                        missing_dates = sorted(raw_dates - agg_dates)
                        print(f"  Missing dates: {missing_dates[:10]}...")  # Show first 10
                        if len(missing_dates) > 10:
                            print(f"    ... and {len(missing_dates) - 10} more")
                    else:
                        print(f"  ✅ All dates present")
                else:
                    print(f"  ❌ No aggregated data found!")
            else:
                print(f"  No raw data found")
        
        # 4. Check for gaps in aggregated data
        print("\n📊 Checking for gaps in aggregated data...")
        for tx_type in raw_transaction_types.keys():
            print(f"\n📅 Checking gaps for: {tx_type}")
            
            cur.execute('''
                SELECT DATE("date") as agg_date
                FROM public."qrl_aggregated_transaction_data"
                WHERE "transaction_type" = %s
                ORDER BY agg_date
            ''', (tx_type,))
            agg_dates = [row[0] for row in cur.fetchall()]
            
            if len(agg_dates) > 1:
                gaps = []
                for i in range(len(agg_dates) - 1):
                    current_date = agg_dates[i]
                    next_date = agg_dates[i + 1]
                    expected_next = current_date + timedelta(days=1)
                    
                    if next_date != expected_next:
                        # Check if there should be data for the missing dates
                        gap_start = current_date + timedelta(days=1)
                        gap_end = next_date - timedelta(days=1)
                        
                        # Check if there's raw data for these gap dates
                        cur.execute('''
                            SELECT COUNT(DISTINCT DATE(block_found_datetime)) as count
                            FROM public."qrl_blockchain_transactions"
                            WHERE "transaction_type" = %s 
                            AND DATE(block_found_datetime) BETWEEN %s AND %s
                        ''', (tx_type, gap_start, gap_end))
                        gap_data_count = cur.fetchone()[0]
                        
                        if gap_data_count > 0:
                            gaps.append(f"{gap_start} to {gap_end} ({gap_data_count} days with data)")
                
                if gaps:
                    print(f"  ⚠️ Found {len(gaps)} gaps:")
                    for gap in gaps[:5]:  # Show first 5 gaps
                        print(f"    • {gap}")
                    if len(gaps) > 5:
                        print(f"    ... and {len(gaps) - 5} more gaps")
                else:
                    print(f"  ✅ No gaps found")
            else:
                print(f"  Not enough data to check for gaps")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in detailed debug: {e}")
        raise

if __name__ == "__main__":
    debug_missing_dates_detailed() 