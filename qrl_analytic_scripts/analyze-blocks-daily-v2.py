import psycopg2
import psycopg2.extras
import sys
import pandas as pd
from datetime import datetime, timedelta
from settings import connection, cur

GENESIS_DATE = datetime(2018, 6, 26)  # QRL blockchain launch date

def fetch_existing_analytics_dates():
    """Fetch all available analytics dates from the aggregated data table."""
    try:
        cur.execute('SELECT DISTINCT "date" FROM public."qrl_aggregated_block_data" ORDER BY "date" ASC')
        result = cur.fetchall()
        return {row[0] for row in result}  # Convert to a set for quick lookup
    except Exception as e:
        print(f"Error fetching existing analytics dates: {e}")
        raise

def find_missing_days():
    """Find missing days between genesis block date and today in the aggregated table."""
    try:
        existing_dates = fetch_existing_analytics_dates()
        all_dates = {GENESIS_DATE + timedelta(days=i) for i in range((datetime.utcnow() - GENESIS_DATE).days + 1)}

        missing_dates = sorted(all_dates - existing_dates)  # Find missing dates
        return missing_dates
    except Exception as e:
        print(f"Error finding missing days: {e}")
        raise

def analyze_missing_days():
    """Recalculate analytics for missing days."""
    try:
        missing_dates = find_missing_days()
        if not missing_dates:
            print("✅ No missing days detected.")
            return

        print(f"⚠️ Missing {len(missing_dates)} days of data. Recalculating...")
        for missing_date in missing_dates:
            print(f"📅 Processing missing date: {missing_date}")
            data = fetch_blockchain_data(start_date=missing_date)

            if not data.empty:
                result = analyze_blocks(data)
                save_to_database(result)
            else:
                print(f"⚠️ No data found for {missing_date}, skipping.")

        print("✅ Recalculation of missing days complete.")
    except Exception as e:
        print(f"Error in missing days recalculation: {e}")

def fetch_latest_analytics_date():
    """Fetch the latest analytics date from the aggregated data table."""
    try:
        cur.execute('SELECT MAX("date") FROM public."qrl_aggregated_block_data"')
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Error fetching latest analytics date: {e}")
        raise

def fetch_blockchain_data(start_date=None):
    """Fetch blockchain data from the blocks table."""
    try:
        query = '''
            SELECT "block_number", "block_found_datetime", "block_size",
                   "block_reward_block", "block_reward_fee"
            FROM public."qrl_blockchain_blocks"
        '''
        if start_date:
            query += ' WHERE "block_found_datetime" >= %s ORDER BY "block_found_datetime" ASC'
            cur.execute(query, (start_date,))
        else:
            query += ' ORDER BY "block_found_datetime" ASC'
            cur.execute(query)

        return pd.DataFrame(cur.fetchall(), columns=[
            "block_number", "block_found_datetime", "block_size",
            "block_reward_block", "block_reward_fee"
        ])
    
    except Exception as e:
        print(f"Error fetching blockchain data: {e}")
        raise

def analyze_blocks(df):
    """Analyze and aggregate block data."""
    try:
        # Convert to datetime and calculate date floor
        df['block_found_datetime'] = pd.to_datetime(df['block_found_datetime'], utc=True)
        df['date'] = df['block_found_datetime'].dt.floor('d')

        # Sort by timestamp and calculate time difference
        df = df.sort_values('block_found_datetime')
        df['time_diff'] = df['block_found_datetime'].diff().dt.total_seconds()

        # Group and aggregate data
        df_grouped = df.groupby('date').agg({
            'block_number': 'count',
            'block_size': ['mean', 'min', 'max'],
            'block_reward_block': ['mean', 'sum'],
            'time_diff': ['mean', 'min', 'max'],
            'block_reward_fee': ['sum', 'mean']
        })

        # Flatten multi-level column names
        df_grouped.columns = ["_".join(col).strip() for col in df_grouped.columns]

        # Add metadata
        df_grouped = df_grouped.reset_index()
        df_grouped['analyze_script_date'] = pd.Timestamp.now()
        df_grouped['analyze_script_name'] = 'analyze-blocks-daily'
        df_grouped['analyze_script_version'] = '0.02'

        # Rename columns to match old format
        df_grouped = df_grouped.rename(columns={
            'time_diff_max': 'block_timestamp_seconds_max',
            'time_diff_min': 'block_timestamp_seconds_min',
            'time_diff_mean': 'block_timestamp_seconds_mean'
        })

        return df_grouped
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise

def save_to_database(df_grouped):
    """Insert aggregated data into the database with upsert logic."""
    tuples = [tuple(row) for row in df_grouped.to_numpy()]
    cols = ', '.join(df_grouped.columns)
    update_cols = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df_grouped.columns if col != 'date'])

    query = f"""
        INSERT INTO public.qrl_aggregated_block_data ({cols})
        VALUES ({', '.join(['%s'] * len(df_grouped.columns))})
        ON CONFLICT ("date")
        DO UPDATE SET {update_cols}
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, tuples)
            connection.commit()
            print(f"✅ Successfully upserted {len(tuples)} rows.")
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        connection.rollback()
        raise

def analyze_qrl_blocks():
    """Run daily analysis on the blockchain data."""
    try:
        latest_date = fetch_latest_analytics_date()
        print(f"Latest analytics date: {latest_date}")

        # Fetch data from the day after the latest analytics date
        start_date = latest_date or GENESIS_DATE  # Default to genesis block date
        data = fetch_blockchain_data(start_date=start_date)

        if not data.empty:
            result = analyze_blocks(data)
            save_to_database(result)
            print("✅ Daily analysis complete.")
        else:
            print("⚠️ No new data available for analysis.")
    except Exception as e:
        print(f"❌ Error in daily analysis: {e}")

def recalculate_all_days():
    """Recalculate analytics for all days."""
    try:
        print("🔄 Starting full reanalysis...")
        data = fetch_blockchain_data()

        if not data.empty:
            result = analyze_blocks(data)
            save_to_database(result)
            print("✅ Full reanalysis complete.")
        else:
            print("⚠️ No data available for reanalysis.")
    except Exception as e:
        print(f"❌ Error in full reanalysis: {e}")

# Command-line execution
if __name__ == "__main__":
    print("Usage:")
    print("  python analyze-blocks-daily-v2.py             # Run daily analysis")
    print("  python analyze-blocks-daily-v2.py recalculate_all  # Full reanalysis")
    print("  python analyze-blocks-daily-v2.py check_missing  # Check and fill missing days")

    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == 'recalculate_all':
                recalculate_all_days()
            elif sys.argv[1] == 'check_missing':
                analyze_missing_days()
            else:
                print("⚠️ Unknown command.")
        else:
            analyze_qrl_blocks()
    except Exception as e:
        print(f"❌ Critical error: {e}")
