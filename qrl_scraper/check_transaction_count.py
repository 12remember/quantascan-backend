import os
import sys
import psycopg2
import environ

# Add the parent directory to the path so we can import the utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def check_transaction_count():
    """Check the current transaction count in the database."""
    
    print("=== Checking Current Transaction Count ===")
    
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
        
        # Get current transaction count
        cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_transactions"')
        transaction_count = cursor.fetchone()[0]
        print(f"📊 Current transaction count: {transaction_count:,}")
        
        # Get count of recent transactions (last 24 hours)
        cursor.execute('''
            SELECT COUNT(*) FROM public."qrl_blockchain_transactions" 
            WHERE "transaction_added_datetime" >= NOW() - INTERVAL '24 hours'
        ''')
        recent_count = cursor.fetchone()[0]
        print(f"📊 Transactions added in last 24 hours: {recent_count:,}")
        
        # Get count of transactions from recent blocks (3751439-3751448)
        cursor.execute('''
            SELECT COUNT(*) FROM public."qrl_blockchain_transactions" 
            WHERE "transaction_block_number" BETWEEN 3751439 AND 3751448
        ''')
        recent_blocks_count = cursor.fetchone()[0]
        print(f"📊 Transactions from recent blocks (3751439-3751448): {recent_blocks_count:,}")
        
        # Get latest transaction
        cursor.execute('''
            SELECT "transaction_hash", "transaction_block_number", "transaction_type", "transaction_added_datetime"
            FROM public."qrl_blockchain_transactions" 
            ORDER BY "transaction_added_datetime" DESC 
            LIMIT 1
        ''')
        latest_tx = cursor.fetchone()
        if latest_tx:
            tx_hash, block_num, tx_type, added_time = latest_tx
            print(f"📊 Latest transaction: {tx_hash[:20]}... | Block {block_num} | {tx_type} | {added_time}")
        
        cursor.close()
        connection.close()
        
        print("\n✅ Transaction count check completed!")
        
    except Exception as e:
        print(f"❌ Error checking transaction count: {e}")
        return False
    
    return True

if __name__ == "__main__":
    check_transaction_count() 