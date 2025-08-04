import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def test_missing_detection():
    """Test if the missing days detection works correctly."""
    try:
        print("🧪 Testing missing days detection...")
        
        # 1. Get current state
        print("\n📊 Current state:")
        cur.execute('''
            SELECT DISTINCT DATE("date") as coinbase_date
            FROM public."qrl_aggregated_transaction_data" 
            WHERE "transaction_type" = 'coinbase'
            ORDER BY coinbase_date ASC
        ''')
        result = cur.fetchall()
        current_aggregated_dates = {row[0] for row in result}
        print(f"📊 Currently have {len(current_aggregated_dates)} coinbase dates in aggregated data")
        
        # 2. Get raw transaction dates
        cur.execute('''
            SELECT DISTINCT DATE(block_found_datetime) as transaction_date
            FROM public."qrl_blockchain_transactions"
            WHERE "transaction_type" = 'coinbase'
            ORDER BY transaction_date ASC
        ''')
        result = cur.fetchall()
        raw_coinbase_dates = {row[0] for row in result}
        print(f"📊 Raw transaction data has {len(raw_coinbase_dates)} coinbase dates")
        
        # 3. Test the current missing days function
        print("\n📊 Testing current missing days function:")
        missing_dates = find_missing_transaction_days()
        print(f"📊 Missing days detected: {len(missing_dates)}")
        
        if missing_dates:
            print(f"📅 First missing: {missing_dates[0]}")
            print(f"📅 Last missing: {missing_dates[-1]}")
        else:
            print("✅ No missing days detected")
        
        # 4. Test with artificial missing dates
        print("\n🧪 Testing with artificial missing dates...")
        
        # Pick a few recent dates to temporarily delete
        recent_dates = sorted(list(current_aggregated_dates))[-5:]  # Last 5 dates
        print(f"📅 Temporarily removing these dates: {recent_dates}")
        
        # Delete these dates from aggregated data
        for date in recent_dates:
            cur.execute('''
                DELETE FROM public."qrl_aggregated_transaction_data" 
                WHERE DATE("date") = %s AND "transaction_type" = 'coinbase'
            ''', (date,))
        
        connection.commit()
        print("✅ Temporarily removed test dates")
        
        # 5. Test missing detection again
        print("\n📊 Testing missing days detection after removal:")
        missing_dates_after = find_missing_transaction_days()
        print(f"📊 Missing days detected: {len(missing_dates_after)}")
        
        if missing_dates_after:
            print(f"📅 Missing dates: {missing_dates_after}")
        else:
            print("❌ No missing days detected - function may not be working")
        
        # 6. Restore the deleted dates
        print("\n🔄 Restoring deleted dates...")
        for date in recent_dates:
            # Re-insert the data for these dates
            start_date = datetime.combine(date, datetime.min.time())
            end_date = datetime.combine(date, datetime.max.time())
            
            # Fetch the original transaction data for this date
            cur.execute('''
                SELECT DISTINCT "transaction_hash",
                       "transaction_block_number",
                       public.qrl_blockchain_transactions.block_found_datetime,
                       "transaction_type", 
                       "transaction_amount_send", 
                       "transaction_result",
                       qrl_blockchain_transactions.master_addr_fee
                FROM public."qrl_blockchain_transactions"
                INNER JOIN public."qrl_blockchain_blocks" 
                ON "transaction_block_number" = "block_number"
                WHERE public.qrl_blockchain_transactions.block_found_datetime >= %s 
                AND public.qrl_blockchain_transactions.block_found_datetime <= %s
                AND "transaction_type" = 'coinbase'
                ORDER BY "transaction_block_number" ASC
            ''', (start_date, end_date))
            
            transactions = cur.fetchall()
            if transactions:
                # Create aggregated data for this date
                df = pd.DataFrame(transactions, columns=[
                    "transaction_hash", "transaction_block_number", "block_found_datetime", 
                    "transaction_type", "transaction_amount_send", "transaction_result", "master_addr_fee"
                ])
                
                # Analyze and insert
                df['date'] = pd.to_datetime(df['block_found_datetime']).dt.floor('d')
                df['master_addr_fee_no_0'] = df.apply(lambda row: np.nan if row['transaction_type'] == 'coinbase' else row['master_addr_fee'], axis=1)
                
                df_grouped = df.groupby(['date', 'transaction_type']).agg({
                    'transaction_block_number': 'count',
                    'transaction_amount_send': ['sum', 'count'],
                    'master_addr_fee_no_0': ['sum', 'mean', 'min', 'max']
                })
                df_grouped.columns = ["_".join(col) for col in df_grouped.columns.ravel()]
                df_grouped = df_grouped.reset_index()
                
                # Add metadata
                df_grouped['analyze_script_date'] = pd.Timestamp.now()
                df_grouped['analyze_script_name'] = 'analyze-transactions-daily'
                df_grouped['analyze_script_version'] = '0.02'
                
                # Rename columns
                df_grouped = df_grouped.rename(columns={
                    'transaction_block_number_count': 'total_blocks_found',
                    'transaction_amount_send_count': 'total_number_of_transactions',
                    'transaction_amount_send_sum': 'total_amount_transfered',
                    'master_addr_fee_no_0_sum': 'transaction_fee_total',
                    'master_addr_fee_no_0_mean': 'transaction_fee_mean',
                    'master_addr_fee_no_0_min': 'transaction_fee_min',
                    'master_addr_fee_no_0_max': 'transaction_fee_max'
                })
                
                # Fill nulls
                df_grouped['transaction_fee_mean'] = df_grouped['transaction_fee_mean'].fillna(0)
                df_grouped['transaction_fee_min'] = df_grouped['transaction_fee_min'].fillna(0)
                df_grouped['transaction_fee_max'] = df_grouped['transaction_fee_max'].fillna(0)
                
                # Insert
                cols = ', '.join(df_grouped.columns)
                placeholders = ', '.join(['%s'] * len(df_grouped.columns))
                values = tuple(df_grouped.iloc[0][col] for col in df_grouped.columns)
                
                cur.execute(f'''
                    INSERT INTO public.qrl_aggregated_transaction_data ({cols})
                    VALUES ({placeholders})
                ''', values)
        
        connection.commit()
        print("✅ Restored deleted dates")
        
        # 7. Final verification
        print("\n📊 Final verification:")
        missing_dates_final = find_missing_transaction_days()
        print(f"📊 Missing days after restoration: {len(missing_dates_final)}")
        
        if len(missing_dates_final) == 0:
            print("✅ Function works correctly - can detect and restore missing dates")
        else:
            print("❌ Function may have issues")
        
        return len(missing_dates_after) == len(recent_dates)
        
    except Exception as e:
        print(f"❌ Error in test: {e}")
        raise

def find_missing_transaction_days():
    """Find missing days between genesis block date and today in the aggregated transaction table."""
    try:
        # Get existing dates that have coinbase transactions in aggregated data
        # Convert timestamp to date for proper comparison
        cur.execute('''
            SELECT DISTINCT DATE("date") as coinbase_date
            FROM public."qrl_aggregated_transaction_data" 
            WHERE "transaction_type" = 'coinbase'
            ORDER BY coinbase_date ASC
        ''')
        result = cur.fetchall()
        existing_coinbase_dates = {row[0] for row in result}
        
        # Get all dates from genesis to today that should have coinbase transactions
        today = datetime.utcnow().date()
        all_dates = {GENESIS_DATE.date() + timedelta(days=i) for i in range((today - GENESIS_DATE.date()).days + 1)}
        
        # Find missing dates (dates that should have coinbase transactions but don't)
        missing_dates = sorted(all_dates - existing_coinbase_dates)
        return missing_dates
    except Exception as e:
        print(f"Error finding missing transaction days: {e}")
        raise

if __name__ == "__main__":
    test_missing_detection() 