import sys
import pandas as pd
import numpy as np
from datetime import datetime
from settings import connection, cur

def fetch_latest_transaction_analytics_date():
    """Fetch the latest date from the transaction analytics table."""
    try:
        cur.execute('SELECT MAX("date") FROM public."qrl_aggregated_transaction_data"')
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Error fetching latest transaction analytics date: {e}")
        raise

def fetch_transaction_data(start_date=None):
    """Fetch transaction data from the blockchain."""
    try:
        if start_date:
            cur.execute('''
                SELECT "transaction_block_number", 
                       public.qrl_blockchain_transactions.block_found_datetime,
                       "transaction_type", 
                       "transaction_amount_send", 
                       "transaction_result",
                       qrl_blockchain_transactions.master_addr_fee
                FROM public."qrl_blockchain_transactions"
                INNER JOIN public."qrl_blockchain_blocks" 
                ON "transaction_block_number" = "block_number"
                WHERE public.qrl_blockchain_transactions.block_found_datetime >= %s
                ORDER BY "transaction_block_number" ASC
            ''', (start_date,))
        else:
            cur.execute('''
                SELECT "transaction_block_number", 
                       public.qrl_blockchain_transactions.block_found_datetime,
                       "transaction_type", 
                       "transaction_amount_send", 
                       "transaction_result",
                       qrl_blockchain_transactions.master_addr_fee
                FROM public."qrl_blockchain_transactions"
                INNER JOIN public."qrl_blockchain_blocks" 
                ON "transaction_block_number" = "block_number"
                ORDER BY "transaction_block_number" ASC
            ''')
        return pd.DataFrame(
            cur.fetchall(),
            columns=[
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
        # Convert timestamps and group by day and type
        df['date'] = pd.to_datetime(df['block_found_datetime']).dt.floor('d')  # Use 'date' to match the table
        df['master_addr_fee_no_0'] = df['master_addr_fee'].replace(0, np.nan)

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
            print(f"Successfully upserted {len(tuples)} rows.")
    except Exception as e:
        print(f"Error saving transaction analysis to database: {e}")
        connection.rollback()
        raise


def analyze_qrl_transactions():
    """Perform daily transaction analysis."""
    try:
        latest_date = fetch_latest_transaction_analytics_date()
        print(f"Latest transaction analytics date found: {latest_date}.")

        start_date = latest_date or datetime(2000, 1, 1)  # Default to 2000 if no data exists
        data = fetch_transaction_data(start_date=start_date)

        if not data.empty:
            result = analyze_transactions(data)
            save_transaction_analysis_to_database_upsert(result)

        print("Daily transaction analysis complete.")
    except Exception as e:
        print(f"Error in daily transaction analysis: {e}")

def recalculate_all_transactions():
    """Recalculate transaction analytics for all data."""
    try:
        print("Starting full transaction reanalysis.")
        data = fetch_transaction_data()

        if not data.empty:
            result = analyze_transactions(data)
            save_transaction_analysis_to_database_upsert(result)

        print("Full transaction reanalysis complete.")
    except Exception as e:
        print(f"Error in full transaction reanalysis: {e}")

# Script entry point
if __name__ == "__main__":
    print("Usage:")
    print("  python analyze_transactions.py             # Run daily transaction analysis")
    print("  python analyze_transactions.py recalculate_all  # Full transaction reanalysis")

    try:
        if len(sys.argv) > 1 and sys.argv[1] == 'recalculate_all':
            recalculate_all_transactions()
        else:
            analyze_qrl_transactions()
    except Exception as e:
        print(f"Critical error in main execution: {e}")
