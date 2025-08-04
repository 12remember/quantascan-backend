import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def fetch_existing_transaction_dates():
    """Fetch all available transaction dates from the aggregated data table."""
    try:
        cur.execute('SELECT DISTINCT "date" FROM public."qrl_aggregated_transaction_data" ORDER BY "date" ASC')
        result = cur.fetchall()
        return {row[0] for row in result}  # Convert to a set for quick lookup
    except Exception as e:
        print(f"Error fetching existing transaction dates: {e}")
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

def analyze_missing_transaction_days():
    """Recalculate transaction analytics for missing days."""
    try:
        missing_dates = find_missing_transaction_days()
        if not missing_dates:
            print("✅ No missing transaction days detected.")
            return

        print(f"⚠️ Missing {len(missing_dates)} transaction days. Recalculating...")
        for missing_date in missing_dates:
            print(f"📅 Processing missing transactions for date: {missing_date}")
            
            # Fetch transaction data for the specific missing date
            start_date = datetime.combine(missing_date, datetime.min.time())
            end_date = datetime.combine(missing_date, datetime.max.time())
            data = fetch_transaction_data_for_date_range(start_date, end_date)

            if not data.empty:
                result = analyze_transactions(data)
                save_transaction_analysis_to_database_upsert_improved(result, missing_date)
            else:
                print(f"⚠️ No transactions found for {missing_date}, skipping.")

        print("✅ Recalculation of missing transaction days complete.")
    except Exception as e:
        print(f"Error in missing transaction days recalculation: {e}")

def fetch_latest_transaction_analytics_date():
    """Fetch the latest date from the transaction analytics table."""
    try:
        cur.execute('SELECT MAX("date") FROM public."qrl_aggregated_transaction_data"')
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Error fetching latest transaction analytics date: {e}")
        raise

def fetch_transaction_data(start_date=None):
    """Fetch transaction data from the blockchain, counting only unique transaction_hash."""
    try:
        query = '''
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
        '''
        if start_date:
            query += ' WHERE public.qrl_blockchain_transactions.block_found_datetime >= %s '
            query += 'ORDER BY "transaction_block_number" ASC'
            cur.execute(query, (start_date,))
        else:
            query += ' ORDER BY "transaction_block_number" ASC'
            cur.execute(query)
        
        return pd.DataFrame(
            cur.fetchall(),
            columns=[
                "transaction_hash",  
                "transaction_block_number", 
                "block_found_datetime", 
                "transaction_type", 
                "transaction_amount_send", 
                "transaction_result", 
                "master_addr_fee"
            ]
        )
    except Exception as e:
        print(f"Error fetching transaction data: {e}")
        raise

def fetch_transaction_data_for_date_range(start_date, end_date):
    """Fetch transaction data from the blockchain within a specific date range."""
    try:
        query = '''
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
            ORDER BY "transaction_block_number" ASC
        '''
        cur.execute(query, (start_date, end_date))
        
        return pd.DataFrame(
            cur.fetchall(),
            columns=[
                "transaction_hash",  
                "transaction_block_number", 
                "block_found_datetime", 
                "transaction_type", 
                "transaction_amount_send", 
                "transaction_result", 
                "master_addr_fee"
            ]
        )
    except Exception as e:
        print(f"Error fetching transaction data for date range: {e}")
        raise

def analyze_transactions(df):
    """Analyze and aggregate transaction data."""
    try:
        df['date'] = pd.to_datetime(df['block_found_datetime']).dt.floor('d')  # Use 'date' to match the table
        
        # Fix transaction fee calculation - don't exclude coinbase transactions from fee calculations
        # Coinbase transactions can have fees too, just set to 0 if not present
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
        df_grouped['analyze_script_version'] = '0.03'  # Updated version

        # Rename columns for consistency with the database schema
        df_grouped = df_grouped.rename(columns={
            'transaction_block_number_count': 'total_blocks_found',
            'transaction_amount_send_count': 'total_number_of_transactions',
            'transaction_amount_send_sum': 'total_amount_transfered',
            'master_addr_fee_no_0_sum': 'transaction_fee_total',
            'master_addr_fee_no_0_mean': 'transaction_fee_mean',
            'master_addr_fee_no_0_min': 'transaction_fee_min',
            'master_addr_fee_no_0_max': 'transaction_fee_max'
        })

        # Ensure nulls are replaced where needed
        df_grouped['transaction_fee_mean'] = df_grouped['transaction_fee_mean'].fillna(0)
        df_grouped['transaction_fee_min'] = df_grouped['transaction_fee_min'].fillna(0)
        df_grouped['transaction_fee_max'] = df_grouped['transaction_fee_max'].fillna(0)

        return df_grouped
    except Exception as e:
        print(f"Error during transaction analysis: {e}")
        raise

def save_transaction_analysis_to_database_upsert(df_grouped):
    """Insert or update aggregated transaction data in the database."""
    tuples = [tuple(row) for row in df_grouped.to_numpy()]
    cols = ', '.join(df_grouped.columns)
    update_cols = ', '.join([
        f'"{col}" = EXCLUDED."{col}"'
        for col in df_grouped.columns if col not in ['date', 'transaction_type']
    ])

    query = f"""
        INSERT INTO public.qrl_aggregated_transaction_data ({cols})
        VALUES ({', '.join(['%s'] * len(df_grouped.columns))})
        ON CONFLICT ("date", "transaction_type")
        DO UPDATE SET {update_cols}
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, tuples)
            connection.commit()
            print(f"✅ Successfully upserted {len(tuples)} rows.")
    except Exception as e:
        print(f"❌ Error saving transaction analysis to database: {e}")
        connection.rollback()
        raise

def save_transaction_analysis_to_database_upsert_improved(df_grouped, current_date):
    """Insert or update aggregated transaction data in the database with improved logic for current date."""
    try:
        with connection.cursor() as cursor:
            for _, row in df_grouped.iterrows():
                # Check if current date and transaction type already exist
                cursor.execute('''
                    SELECT COUNT(*) FROM public.qrl_aggregated_transaction_data 
                    WHERE "date" = %s AND "transaction_type" = %s
                ''', (row['date'], row['transaction_type']))
                
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    # Update existing record for current date
                    update_cols = ', '.join([
                        f'"{col}" = %s'
                        for col in df_grouped.columns if col not in ['date', 'transaction_type']
                    ])
                    
                    update_values = [row[col] for col in df_grouped.columns if col not in ['date', 'transaction_type']]
                    update_values.extend([row['date'], row['transaction_type']])  # Add WHERE clause values
                    
                    query = f"""
                        UPDATE public.qrl_aggregated_transaction_data 
                        SET {update_cols}
                        WHERE "date" = %s AND "transaction_type" = %s
                    """
                    
                    cursor.execute(query, update_values)
                    if current_date:
                        print(f"🔄 Updated existing record for {row['date']} - {row['transaction_type']}")
                else:
                    # Insert new record
                    cols = ', '.join(df_grouped.columns)
                    placeholders = ', '.join(['%s'] * len(df_grouped.columns))
                    values = tuple(row[col] for col in df_grouped.columns)
                    
                    query = f"""
                        INSERT INTO public.qrl_aggregated_transaction_data ({cols})
                        VALUES ({placeholders})
                    """
                    
                    cursor.execute(query, values)
                    if current_date:
                        print(f"➕ Inserted new record for {row['date']} - {row['transaction_type']}")
            
            connection.commit()
            if current_date:
                print(f"✅ Successfully processed {len(df_grouped)} rows with improved upsert logic.")
            else:
                print(f"✅ Successfully processed {len(df_grouped)} rows for full recalculation.")
            
    except Exception as e:
        print(f"❌ Error saving transaction analysis to database: {e}")
        connection.rollback()
        raise

def recalculate_between_dates(start_date, end_date):
    """Recalculate transaction analytics between two specific dates."""
    try:
        print(f"🔄 Recalculating transactions between {start_date} and {end_date}...")
        
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Delete existing aggregated data for this date range
        cur.execute('''
            DELETE FROM public."qrl_aggregated_transaction_data" 
            WHERE DATE("date") BETWEEN %s AND %s
        ''', (start_date, end_date))
        connection.commit()
        print(f"✅ Cleared existing aggregated data for date range")
        
        # Fetch transaction data for the date range
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        data = fetch_transaction_data_for_date_range(start_datetime, end_datetime)
        
        if data.empty:
            print("⚠️ No transaction data found for the specified date range.")
            return
        
        print(f"📊 Processing {len(data)} transactions for date range...")
        
        # Analyze and aggregate the data
        result = analyze_transactions(data)
        
        # Save to database
        save_transaction_analysis_to_database_upsert_improved(result, None)
        
        print(f"✅ Successfully recalculated transactions between {start_date} and {end_date}")
        
    except Exception as e:
        print(f"❌ Error recalculating between dates: {e}")
        raise

def analyze_qrl_transactions():
    """Main function to analyze QRL transactions for daily aggregation."""
    try:
        print("🚀 Starting daily QRL transaction analysis...")
        
        # Get the current date (today)
        current_date = datetime.utcnow().date()
        print(f"📅 Analyzing transactions for current date: {current_date}")
        
        # Fetch transaction data for the current day
        start_date = datetime.combine(current_date, datetime.min.time())
        end_date = datetime.combine(current_date, datetime.max.time())
        
        # Fetch transaction data for the current day
        data = fetch_transaction_data_for_date_range(start_date, end_date)
        
        if data.empty:
            print("✅ No transactions found for current day.")
            return
        
        print(f"📊 Processing {len(data)} transactions for current day...")
        
        # Analyze and aggregate the data
        result = analyze_transactions(data)
        
        # Save to database with improved upsert logic
        save_transaction_analysis_to_database_upsert(result)
        
        print("✅ Daily transaction analysis complete.")
        
    except Exception as e:
        print(f"❌ Error in daily transaction analysis: {e}")
        raise

def recalculate_all_transactions():
    try:
        print("Recalculating all transaction days from genesis date to now...")
        # Optionally, clear the existing aggregated data
        cur.execute('TRUNCATE public."qrl_aggregated_transaction_data"')
        connection.commit()
        print("Existing aggregated data cleared.")

        # Fetch all transaction data starting from the genesis date
        data = fetch_transaction_data(start_date=GENESIS_DATE)
        if data.empty:
            print("⚠️ No transaction data found from genesis date onward.")
            return

        # Analyze and aggregate the entire dataset
        result = analyze_transactions(data)
        save_transaction_analysis_to_database_upsert_improved(result, None)  # None for recalculate_all
        print("✅ Successfully recalculated all transaction days.")
    except Exception as e:
        print(f"Error recalculating all transactions: {e}")


# Script entry point
if __name__ == "__main__":
    print("Usage:")
    print("  python analyze-transactions-daily-v2.py                                    # Run daily transaction analysis")
    print("  python analyze-transactions-daily-v2.py recalculate_all                    # Full transaction reanalysis")
    print("  python analyze-transactions-daily-v2.py check_missing                      # Check and fill missing transaction days")
    print("  python analyze-transactions-daily-v2.py recalculate_between 2025-07-26 2025-07-30  # Recalculate between specific dates")

    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == 'recalculate_all':
                recalculate_all_transactions()
            elif sys.argv[1] == 'check_missing':
                analyze_missing_transaction_days()
            elif sys.argv[1] == 'recalculate_between':
                if len(sys.argv) >= 4:
                    start_date = sys.argv[2]
                    end_date = sys.argv[3]
                    recalculate_between_dates(start_date, end_date)
                else:
                    print("⚠️ Usage: python analyze-transactions-daily-v2.py recalculate_between START_DATE END_DATE")
                    print("   Example: python analyze-transactions-daily-v2.py recalculate_between 2025-07-26 2025-07-30")
            else:
                print("⚠️ Unknown command.")
        else:
            # This is the fix - call the correct function for daily analysis
            analyze_qrl_transactions()
    except Exception as e:
        print(f"❌ Critical error in main execution: {e}")