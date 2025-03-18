import sys
import pandas as pd
from settings import connection, cur

def fetch_wallets_without_first_block():
    """Fetch wallets that do not have the first block information."""
    try:
        cur.execute('''SELECT "wallet_address", "address_first_found", "address_first_found_block_num"
                       FROM public."qrl_wallet_address"
                       WHERE address_first_found_block_num IS NULL''')
        return cur.fetchall()
    except Exception as e:
        print(f"Error fetching wallets without first block: {e}")
        raise

def fetch_all_wallets():
    """Fetch all wallets from the database."""
    try:
        cur.execute('''SELECT "wallet_address", "address_first_found", "address_first_found_block_num"
                       FROM public."qrl_wallet_address"''')
        return cur.fetchall()
    except Exception as e:
        print(f"Error fetching all wallets: {e}")
        raise

def fetch_first_transaction(wallet):
    """Fetch the first transaction for a given wallet."""
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
    """Update the wallet with its first transaction block details."""
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
    """
    Update wallets that are missing first block info.
    This should be run when a wallet is used for the first time.
    """
    try:
        wallets = fetch_wallets_without_first_block()
        if not wallets:
            print("No wallets to update (missing first block info).")
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

def recheck_wallet_first_block(wallet):
    """
    Recheck and update a specific wallet's first block information.
    This is useful if you suspect that the initially stored value is incorrect.
    """
    try:
        first_transaction = fetch_first_transaction(wallet)
        if first_transaction:
            block_number, block_datetime = first_transaction
            # Fetch the stored record for comparison
            cur.execute('''SELECT "address_first_found", "address_first_found_block_num"
                           FROM public."qrl_wallet_address"
                           WHERE "wallet_address" = %s''', (wallet,))
            stored = cur.fetchone()
            if stored:
                stored_datetime, stored_block_number = stored
                if stored_block_number != block_number:
                    print(f"Wallet {wallet} stored block {stored_block_number} differs from actual first block {block_number}. Updating...")
                    update_wallet_first_block(wallet, block_datetime, block_number)
                else:
                    print(f"Wallet {wallet} is already up-to-date.")
            else:
                print(f"Wallet {wallet} not found in the database.")
        else:
            print(f"No transactions found for wallet {wallet}.")
    except Exception as e:
        print(f"Error rechecking wallet {wallet}: {e}")

def recheck_all_wallets():
    """
    Recheck all wallets and update their first block info if needed.
    This function iterates over every wallet in the database.
    """
    try:
        wallets = fetch_all_wallets()
        if not wallets:
            print("No wallets found for recheck.")
            return

        for wallet_data in wallets:
            wallet = wallet_data[0]
            try:
                recheck_wallet_first_block(wallet)
            except Exception as e:
                print(f"Error processing wallet {wallet}: {e}")

        print("Recheck of all wallets complete.")
    except Exception as e:
        print(f"Error in recheck_all_wallets: {e}")

if __name__ == "__main__":
    print("Starting wallet stats update...")
    try:
        # Update wallets missing first block info
        add_wallet_stats()

        # Optionally, to recheck all wallets uncomment the next line:
        recheck_all_wallets()
    except Exception as e:
        print(f"Critical error in wallet stats update: {e}")
