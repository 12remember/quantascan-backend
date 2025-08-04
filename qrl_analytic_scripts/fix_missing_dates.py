import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

def fix_missing_dates():
    """Fix the specific missing dates found in the debug."""
    try:
        print("🔧 Fixing specific missing dates...")
        
        # Missing dates found from debug:
        # Slave transactions: 2025-07-26, 2025-07-27, 2025-07-29, 2025-07-30
        # Token transactions: 2025-04-05
        
        missing_slave_dates = [
            datetime(2025, 7, 26).date(),
            datetime(2025, 7, 27).date(),
            datetime(2025, 7, 29).date(),
            datetime(2025, 7, 30).date()
        ]
        
        missing_token_dates = [
            datetime(2025, 4, 5).date()
        ]
        
        print(f"📅 Fixing {len(missing_slave_dates)} missing slave transaction dates")
        print(f"📅 Fixing {len(missing_token_dates)} missing token transaction dates")
        
        # Fix slave transaction dates
        for date in missing_slave_dates:
            print(f"🔄 Processing slave transactions for {date}...")
            start_date = datetime.combine(date, datetime.min.time())
            end_date = datetime.combine(date, datetime.max.time())
            
            # Fetch slave transactions for this date
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
                AND "transaction_type" = 'slave'
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
                df['master_addr_fee_no_0'] = df['master_addr_fee'].fillna(0)
                
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
                df_grouped['analyze_script_version'] = '0.03'
                
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
                
                print(f"✅ Added slave transaction data for {date}")
            else:
                print(f"⚠️ No slave transactions found for {date}")
        
        # Fix token transaction dates
        for date in missing_token_dates:
            print(f"🔄 Processing token transactions for {date}...")
            start_date = datetime.combine(date, datetime.min.time())
            end_date = datetime.combine(date, datetime.max.time())
            
            # Fetch token transactions for this date
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
                AND "transaction_type" = 'token'
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
                df['master_addr_fee_no_0'] = df['master_addr_fee'].fillna(0)
                
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
                df_grouped['analyze_script_version'] = '0.03'
                
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
                
                print(f"✅ Added token transaction data for {date}")
            else:
                print(f"⚠️ No token transactions found for {date}")
        
        connection.commit()
        print("✅ Successfully fixed all missing dates!")
        
    except Exception as e:
        print(f"❌ Error fixing missing dates: {e}")
        raise

if __name__ == "__main__":
    fix_missing_dates() 