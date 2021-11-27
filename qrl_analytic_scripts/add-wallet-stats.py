
import psycopg2
import psycopg2.extras

import os
import pandas as pd
import numpy as np
from django_pandas.io import read_frame
import datetime as dt
import sys

from settings import connection , cur

def add_wallet_stats():
    cur.execute('SELECT "wallet_address", "address_first_found","address_first_found_block_num" FROM public."qrl_wallet_address" WHERE address_first_found_block_num IS NULL')     
    wallets_without_first_block_found = cur.fetchall()
    for walletData in wallets_without_first_block_found:
        wallet = walletData[0]        
        try:
            cur.execute('SELECT "transaction_block_number", "block_found_datetime" FROM public."qrl_blockchain_transactions" WHERE "transaction_receiving_wallet_address" = %s OR "transaction_sending_wallet_address" = %s ORDER BY "transaction_block_number" ASC LIMIT 1', (wallet,wallet))
            wallet_first_found = cur.fetchone()
            
            transaction_block_number = wallet_first_found[0]
            block_found_datetime = wallet_first_found[1]
            print(block_found_datetime)
            cur.execute('UPDATE public. "qrl_wallet_address" SET "address_first_found" = %s, "address_first_found_block_num" = %s\
            WHERE "wallet_address" = %s', (block_found_datetime, transaction_block_number,wallet ))                    
            connection.commit()
            print('success')
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            connection.rollback()
            cur.close()
            return 1
    cur.close()


add_wallet_stats()

