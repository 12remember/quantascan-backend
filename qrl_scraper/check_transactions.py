import os
import sys
import psycopg2
import environ
from datetime import datetime, timezone

# Add the parent directory to the path so we can import the utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def check_transactions():
    """Check the current state of transactions in the database."""
    
    print("=== Checking Transactions in Database ===")
    
    # Get database configuration
    DJANGO_ENV = env("DJANGO_ENV", default="development")
    USE_PROD_DB = env.bool("USE_PROD_DB", default=False)
    
    print(f"DJANGO_ENV: {DJANGO_ENV}")
    print(f"USE_PROD_DB: {USE_PROD_DB}")
    
    try:
        if DJANGO_ENV == "production" or USE_PROD_DB:
            database_url = env("DATABASE_URL")
            print(f"Connecting to production database...")
            connection = psycopg2.connect(database_url)
        else:
            hostname = env('DEV_DB_HOST', default='127.0.0.1')
            username = env('DEV_DB_USER', default='dev_user')
            password = env('DEV_DB_PASSWORD', default='dev_password')
            database = env('DEV_DB_NAME', default='qrl_dev')
            port = env('DEV_DB_PORT', default='5432')
            
            print(f"Connecting to development database: {hostname}:{port}/{database}")
            connection = psycopg2.connect(
                host=hostname,
                user=username,
                password=password,
                dbname=database,
                port=port
            )
        
        cursor = connection.cursor()
        
        # Check blocks table
        cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_blocks"')
        block_count = cursor.fetchone()[0]
        print(f"Total blocks in database: {block_count}")
        
        # Check transactions table
        cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_transactions"')
        transaction_count = cursor.fetchone()[0]
        print(f"Total transactions in database: {transaction_count}")
        
        # Get the latest blocks
        cursor.execute('SELECT "block_number", "block_found_datetime" FROM public."qrl_blockchain_blocks" ORDER BY "block_number" DESC LIMIT 5')
        latest_blocks = cursor.fetchall()
        print(f"\nLatest 5 blocks:")
        for block_num, block_time in latest_blocks:
            print(f"  Block {block_num}: {block_time}")
        
        # Get the latest transactions
        cursor.execute('SELECT "transaction_hash", "transaction_block_number", "transaction_type", "transaction_amount_send" FROM public."qrl_blockchain_transactions" ORDER BY "transaction_added_datetime" DESC LIMIT 10')
        latest_transactions = cursor.fetchall()
        print(f"\nLatest 10 transactions:")
        for tx_hash, block_num, tx_type, amount in latest_transactions:
            print(f"  {tx_hash[:20]}... | Block {block_num} | {tx_type} | {amount}")
        
        # Check for transactions from recent blocks (3751439-3751448)
        cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_transactions" WHERE "transaction_block_number" BETWEEN 3751439 AND 3751448')
        recent_transaction_count = cursor.fetchone()[0]
        print(f"\nTransactions from recent blocks (3751439-3751448): {recent_transaction_count}")
        
        # Check for any missed items
        cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_missed_items"')
        missed_count = cursor.fetchone()[0]
        print(f"Missed items in database: {missed_count}")
        
        if missed_count > 0:
            cursor.execute('SELECT "error_name", "item_url" FROM public."qrl_blockchain_missed_items" ORDER BY "error_timestamp" DESC LIMIT 5')
            recent_missed = cursor.fetchall()
            print(f"\nRecent missed items:")
            for error_name, item_url in recent_missed:
                print(f"  {error_name}: {item_url}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error checking transactions: {e}")
        return False
    
    return True

if __name__ == "__main__":
    check_transactions() 