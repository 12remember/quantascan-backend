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

def test_database_connection():
    """Test if we can connect to the database and insert a test record."""
    
    print("=== Testing Database Connection ===")
    
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
        
        print("✅ Database connection successful!")
        
        # Test if we can query the transactions table
        cursor = connection.cursor()
        
        # Check if the table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'qrl_blockchain_transactions'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        print(f"qrl_blockchain_transactions table exists: {table_exists}")
        
        if table_exists:
            # Count existing transactions
            cursor.execute('SELECT COUNT(*) FROM public."qrl_blockchain_transactions"')
            count = cursor.fetchone()[0]
            print(f"Current transaction count: {count}")
            
            # Try to insert a test transaction
            test_transaction = {
                'transaction_hash': 'TEST_HASH_123',
                'transaction_sending_wallet_address': 'Q0105008e06cb4c3b33bc7c7f22a53f6620a4ff2d9b23d7274947a80d08bf99314281337dfc0f93',
                'transaction_receiving_wallet_address': 'Q010500b8601fb018af63f22b31854f649f32249ffd7c2e887d80694b458bd18ee6ca9f9806c016',
                'transaction_amount_send': 1000000000,
                'transaction_type': 'transfer',
                'transaction_block_number': 999999,
                'transaction_found': True,
                'transaction_result': 'transaction',
                'spider_name': 'test_spider',
                'spider_version': '1.0',
                'master_addr_type': 'Buffer',
                'master_addr_data': '0105008e06cb4c3b33bc7c7f22a53f6620a4ff2d9b23d7274947a80d08bf99314281337dfc0f93',
                'master_addr_fee': 0,
                'public_key_type': 'Buffer',
                'public_key_data': 'TEST_PUBLIC_KEY',
                'signature_type': 'Buffer',
                'transaction_nonce': 1,
                'transaction_addrs_to_type': 'Buffer',
                'block_found_datetime': datetime.now(timezone.utc),
                'transaction_added_datetime': datetime.now(timezone.utc)
            }
            
            try:
                cursor.execute("""
                    INSERT INTO public."qrl_blockchain_transactions" (
                        "transaction_hash", "transaction_sending_wallet_address", "transaction_receiving_wallet_address",
                        "transaction_amount_send", "transaction_type", "transaction_block_number",
                        "transaction_found", "transaction_result", "spider_name", "spider_version",
                        "master_addr_type", "master_addr_data", "master_addr_fee", "public_key_type",
                        "public_key_data", "signature_type", "transaction_nonce", "transaction_addrs_to_type",
                        "block_found_datetime", "transaction_added_datetime"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    test_transaction['transaction_hash'],
                    test_transaction['transaction_sending_wallet_address'],
                    test_transaction['transaction_receiving_wallet_address'],
                    test_transaction['transaction_amount_send'],
                    test_transaction['transaction_type'],
                    test_transaction['transaction_block_number'],
                    test_transaction['transaction_found'],
                    test_transaction['transaction_result'],
                    test_transaction['spider_name'],
                    test_transaction['spider_version'],
                    test_transaction['master_addr_type'],
                    test_transaction['master_addr_data'],
                    test_transaction['master_addr_fee'],
                    test_transaction['public_key_type'],
                    test_transaction['public_key_data'],
                    test_transaction['signature_type'],
                    test_transaction['transaction_nonce'],
                    test_transaction['transaction_addrs_to_type'],
                    test_transaction['block_found_datetime'],
                    test_transaction['transaction_added_datetime']
                ))
                
                connection.commit()
                print("✅ Test transaction inserted successfully!")
                
                # Clean up - delete the test transaction
                cursor.execute('DELETE FROM public."qrl_blockchain_transactions" WHERE "transaction_hash" = %s', ('TEST_HASH_123',))
                connection.commit()
                print("✅ Test transaction cleaned up!")
                
            except Exception as e:
                print(f"❌ Error inserting test transaction: {e}")
                connection.rollback()
        
        cursor.close()
        connection.close()
        print("✅ Database connection test completed successfully!")
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_database_connection() 