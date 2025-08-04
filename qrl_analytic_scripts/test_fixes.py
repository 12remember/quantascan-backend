import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def test_missing_days_calculation():
    """Test the fixed missing days calculation."""
    try:
        print("🧪 Testing missing days calculation...")
        
        # Get existing dates that have coinbase transactions
        cur.execute('''
            SELECT DISTINCT "date" 
            FROM public."qrl_aggregated_transaction_data" 
            WHERE "transaction_type" = 'coinbase'
            ORDER BY "date" ASC
        ''')
        result = cur.fetchall()
        existing_coinbase_dates = {row[0] for row in result}
        
        print(f"📊 Found {len(existing_coinbase_dates)} existing coinbase dates")
        
        if existing_coinbase_dates:
            print(f"📅 First coinbase date: {min(existing_coinbase_dates)}")
            print(f"📅 Last coinbase date: {max(existing_coinbase_dates)}")
        
        # Get all dates from genesis to today that should have coinbase transactions
        today = datetime.utcnow().date()
        all_dates = {GENESIS_DATE.date() + timedelta(days=i) for i in range((today - GENESIS_DATE.date()).days + 1)}
        
        # Find missing dates (dates that should have coinbase transactions but don't)
        missing_dates = sorted(all_dates - existing_coinbase_dates)
        
        print(f"📊 Total possible days from genesis to today: {len(all_dates)}")
        print(f"📊 Missing coinbase days: {len(missing_dates)}")
        
        if missing_dates:
            print(f"📅 First missing date: {missing_dates[0]}")
            print(f"📅 Last missing date: {missing_dates[-1]}")
            
            # Show some sample missing dates
            print("📅 Sample missing dates:")
            for i, date in enumerate(missing_dates[:10]):
                print(f"  {i+1}. {date}")
            if len(missing_dates) > 10:
                print(f"  ... and {len(missing_dates) - 10} more")
        
        return len(missing_dates)
        
    except Exception as e:
        print(f"❌ Error in test: {e}")
        raise

def test_current_day_logic():
    """Test the current day analysis logic."""
    try:
        print("\n🧪 Testing current day analysis logic...")
        
        # Get the current date (today)
        current_date = datetime.utcnow().date()
        print(f"📅 Current date: {current_date}")
        
        # Fetch transaction data for the current day
        start_date = datetime.combine(current_date, datetime.min.time())
        end_date = datetime.combine(current_date, datetime.max.time())
        
        print(f"📅 Fetching transactions from {start_date} to {end_date}")
        
        # Test the query
        query = '''
            SELECT COUNT(*) as transaction_count
            FROM public."qrl_blockchain_transactions"
            INNER JOIN public."qrl_blockchain_blocks" 
            ON "transaction_block_number" = "block_number"
            WHERE public.qrl_blockchain_transactions.block_found_datetime >= %s 
            AND public.qrl_blockchain_transactions.block_found_datetime <= %s
        '''
        cur.execute(query, (start_date, end_date))
        result = cur.fetchone()
        
        print(f"📊 Found {result[0]} transactions for current day")
        
        return result[0]
        
    except Exception as e:
        print(f"❌ Error in test: {e}")
        raise

if __name__ == "__main__":
    print("🔍 Testing the fixes for analyze-transactions-daily-v2.py")
    print("=" * 60)
    
    try:
        missing_count = test_missing_days_calculation()
        current_day_count = test_current_day_logic()
        
        print("\n" + "=" * 60)
        print("📋 Test Results Summary:")
        print(f"  • Missing coinbase days: {missing_count}")
        print(f"  • Current day transactions: {current_day_count}")
        
        if missing_count < 100:
            print("✅ Missing days calculation looks reasonable (< 100 days)")
        else:
            print("⚠️ Missing days calculation shows many missing days - may need investigation")
            
        if current_day_count > 0:
            print("✅ Current day has transactions to process")
        else:
            print("⚠️ No transactions found for current day")
            
    except Exception as e:
        print(f"❌ Test failed: {e}") 