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
        existing_dates = fetch_existing_transaction_dates()
        all_dates = {GENESIS_DATE + timedelta(days=i) for i in range((datetime.utcnow() - GENESIS_DATE).days + 1)}

        missing_dates = sorted(all_dates - existing_dates)  # Find missing dates
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
            data = fetch_transaction_data(start_date=missing_date)

            if not data.empty:
                result = analyze_transactions(data)
                save_transaction_analysis_to_database_upsert(result)
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

def analyze_transactions(df):
    """Analyze and aggregate transaction data."""
    try:
        df['date'] = pd.to_datetime(df['block_found_datetime']).dt.floor('d')  # Use 'date' to match the table
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

def analyze_qrl_transactions():
    """Main function to analyze QRL transactions for daily aggregation."""
    try:
        print("🚀 Starting daily QRL transaction analysis...")
        
        # Get the latest date from aggregated data
        latest_date = fetch_latest_transaction_analytics_date()
        
        if latest_date:
            # Start from the day after the latest date
            start_date = latest_date + timedelta(days=1)
            print(f"📅 Fetching transactions from {start_date} onwards...")
        else:
            # No existing data, start from genesis
            start_date = GENESIS_DATE
            print(f"📅 No existing data found. Starting from genesis date: {start_date}")
        
        # Fetch new transaction data
        data = fetch_transaction_data(start_date=start_date)
        
        if data.empty:
            print("✅ No new transactions to process.")
            return
        
        print(f"📊 Processing {len(data)} transactions...")
        
        # Analyze and aggregate the data
        result = analyze_transactions(data)
        
        # Save to database
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
        save_transaction_analysis_to_database_upsert(result)
        print("✅ Successfully recalculated all transaction days.")
    except Exception as e:
        print(f"Error recalculating all transactions: {e}")


# Script entry point
if __name__ == "__main__":
    print("Usage:")
    print("  python analyze-transactions-daily-v2.py             # Run daily transaction analysis")
    print("  python analyze-transactions-daily-v2.py recalculate_all  # Full transaction reanalysis")
    print("  python analyze-transactions-daily-v2.py check_missing  # Check and fill missing transaction days")

    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == 'recalculate_all':
                recalculate_all_transactions()
            elif sys.argv[1] == 'check_missing':
                analyze_missing_transaction_days()
            else:
                print("⚠️ Unknown command.")
        else:
            # This is the fix - call the correct function for daily analysis
            analyze_qrl_transactions()
    except Exception as e:
        print(f"❌ Critical error in main execution: {e}")