import sys
import pandas as pd
from settings import connection, cur

def fetch_wallets_without_first_block():
    try:
        cur.execute('''SELECT "wallet_address", "address_first_found", "address_first_found_block_num"
                       FROM public."qrl_wallet_address"
                       WHERE address_first_found_block_num IS NULL''')
        return cur.fetchall()
    except Exception as e:
        print(f"Error fetching wallets without first block: {e}")
        raise

def fetch_first_transaction(wallet):
    try:
        cur.execute('''SELECT "transaction_block_number", "block_found_datetime"
                       FROM public."qrl_blockchain_transactions"
                       WHERE "transaction_receiving_wallet_address" = %s
                          OR "transaction_sending_wallet_address" = %s
                       ORDER BY "transaction_block_number" ASC
                       LIMIT 1''', (wallet, wallet))
        return cur.fetchone()
    except Exception as e:
        print(f"Error fetching first transaction for wallet {wallet}: {e}")
        raise

def update_wallet_first_block(wallet, block_datetime, block_number):
    try:
        cur.execute('''UPDATE public."qrl_wallet_address"
                       SET "address_first_found" = %s, "address_first_found_block_num" = %s
                       WHERE "wallet_address" = %s''', (block_datetime, block_number, wallet))
        connection.commit()
        print(f"Updated wallet {wallet} with first block {block_number} at {block_datetime}.")
    except Exception as e:
        print(f"Error updating wallet {wallet}: {e}")
        connection.rollback()
        raise

def add_wallet_stats():
    try:
        wallets = fetch_wallets_without_first_block()
        if not wallets:
            print("No wallets to update.")
            return

        for wallet_data in wallets:
            wallet = wallet_data[0]
            try:
                first_transaction = fetch_first_transaction(wallet)
                if first_transaction:
                    block_number, block_datetime = first_transaction
                    update_wallet_first_block(wallet, block_datetime, block_number)
                else:
                    print(f"No transactions found for wallet {wallet}.")
            except Exception as e:
                print(f"Error processing wallet {wallet}: {e}")

        print("Wallet stats update complete.")
    except Exception as e:
        print(f"Error in add_wallet_stats: {e}")

# Instructions for running the script
if __name__ == "__main__":
    print("Starting wallet stats update...")
    try:
        add_wallet_stats()
    except Exception as e:
        print(f"Critical error in wallet stats update: {e}")
