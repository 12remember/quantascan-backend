import psycopg2
import psycopg2.extras
import logging
import traceback
import os
import sys
import json

from datetime import datetime, timezone  


#from scrapy.conf import settings
from scrapy.exceptions import DropItem
 
from .items import QRLNetworkBlockItem, QRLNetworkTransactionItem, QRLNetworkAddressItem, QRLNetworkMissedItem
from .utils import get_db_connection

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')


def handle_spider_error(spider, error, item, item_url="N/A"):
    """Handles all database errors and logs them into the missed items table."""
    
    trace_back = traceback.format_exc()
    error_type = str(type(error))
    error_message = str(error)
    spider_class = getattr(spider, "__class__", "UnknownClass").__name__

    try:
        connection, cur = get_db_connection()
        cur.execute(
            'INSERT INTO public.qrl_blockchain_missed_items ('  # ✅ Ensure it's saving the full transaction details
            '"spider_name", "spider_version", "location_script_file", '
            '"location_script_function", "trace_back", "error_type", '
            '"error_name", "item_url", "error_timestamp", "failed_data") '  # 🔥 NEW: Save the full transaction item as JSON
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (
                spider.name,
                spider.version,
                str(__name__),
                spider_class,
                json.dumps(trace_back),
                error_type,
                error_message,
                item_url,
                datetime.now(timezone.utc),
                json.dumps(item)  # ✅ Save the full transaction data
            )
        )
        connection.commit()
        logging.warning(f"Transaction failed and logged in missed items table: {item.get('transaction_hash', 'UNKNOWN')}")
    except Exception as e:
        connection.rollback()
        logging.error(f"Critical error logging to missed items table: {e}")
    finally:
        connection.close()  # ✅ Ensure the connection is always closed




class QrlnetworkPipeline_block:
    def open_spider(self, spider):
        """Establish a fresh database connection for this pipeline."""
        self.connection, self.cur = get_db_connection()


    def close_spider(self, spider):
        """Close database connection when the spider stops."""
        if not self.cur.closed:
            self.cur.close()
        if not self.connection.closed:
            self.connection.close()



        
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkBlockItem):
            return item
        if self.cur.closed:
            self.connection, self.cur = get_db_connection()

        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]

        if missing_fields:
            logging.warning(f"Block {item.get('block_number', 'UNKNOWN')} is missing fields: {missing_fields}")
            for field in missing_fields:
                item[field] = "MISSING"  # ✅ Mark missing fields but still save the item

        try:
            datetimeNow = datetime.now(timezone.utc)
            self.cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" WHERE "block_number" = %s', (int(item['block_number']),))
            result = self.cur.fetchone()
            if result is None: 
                convert_timestamp_to_datetime = datetime.fromtimestamp(int(item["block_found_datetime"])).strftime("%Y-%m-%d %H:%M:%S")  
                self.cur.execute('INSERT INTO public."qrl_blockchain_blocks" (\
                "block_number", "block_found", "block_result",\
                "block_found_datetime", "block_found_timestamp_seconds", "block_reward_block", "block_reward_fee",\
                "block_mining_nonce", "block_number_of_transactions","spider_name",\
                "spider_version", "block_size", "block_hash_header_type" , "block_hash_header_data",\
                "block_hash_header_type_prev" , "block_hash_header_data_prev", "block_merkle_root_type",\
                "block_merkle_root_data", "block_added_timestamp"\
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s , %s)',
                (int(item['block_number']), item['block_found'],item['block_result'], 
                convert_timestamp_to_datetime, item['block_found_timestamp_seconds'], int(item["block_reward_block"]), int(item["block_reward_fee"]),
                int(item["block_mining_nonce"]), int(item["block_number_of_transactions"]), item["spider_name"],
                item["spider_version"], int(item["block_size"]), item["block_hash_header_type"],
                item["block_hash_header_data"],item["block_hash_header_type_prev"],item["block_hash_header_data_prev"],
                item["block_merkle_root_type"], item["block_merkle_root_data"], datetimeNow ))            
                self.connection.commit()
                logging.warning('Got new block, number: %s ' % item['block_number'])
            else:
                raise DropItem("Already Got Blocknumber: %s" % item['block_number'])

        except DropItem as duplicate :
            logging.info(duplicate)
            
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item.get("item_url", "N/A"))

            self.connection.rollback()

        return item

     

class QrlnetworkPipeline_transaction:
    def open_spider(self, spider):
        """Establish a fresh database connection for this pipeline."""
        self.connection, self.cur = get_db_connection()


    def close_spider(self, spider):
        """Close database connection when the spider stops."""
        if not self.cur.closed:
            self.cur.close()
        if not self.connection.closed:
            self.connection.close()

            
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkTransactionItem):
            return item
        
        if self.cur.closed:
            self.connection, self.cur = get_db_connection()

        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]

        if missing_fields:
            logging.warning(f"Transaction {item.get('transaction_hash', 'UNKNOWN')} is missing fields: {missing_fields}")
            for field in missing_fields:
                item[field] = "MISSING"  # ✅ Mark missing fields but still save the item
                                    
        try:
            datetimeNow = datetime.now(timezone.utc)
            # Check for duplicates
            self.cur.execute(
                'SELECT "transaction_hash" FROM public."qrl_blockchain_transactions" WHERE "transaction_hash" = %s AND "transaction_receiving_wallet_address" = %s',
                (item.get('transaction_hash'), item.get('transaction_receiving_wallet_address'))
            )
            result = self.cur.fetchone()
            if result is None: 
                # Convert timestamp to datetime format
                convert_timestamp_to_datetime = datetime.fromtimestamp(
                    int(item.get("block_found_datetime", 0))
                ).strftime("%Y-%m-%d %H:%M:%S")

                # Prepare the INSERT query
                self.cur.execute(
                    'INSERT INTO public."qrl_blockchain_transactions" ('
                    '"transaction_hash", "transaction_sending_wallet_address", "transaction_receiving_wallet_address", '
                    '"transaction_amount_send", "transaction_type", "transaction_block_number", '
                    '"transaction_found", "transaction_result", "spider_name", '
                    '"spider_version", "master_addr_type", "master_addr_data", '
                    '"master_addr_fee", "public_key_type", "public_key_data", '
                    '"signature_type", "transaction_nonce", '
                    '"transaction_addrs_to_type", "block_found_datetime", '
                    '"transaction_added_datetime"'
                    ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (
                        str(item.get('transaction_hash', 'UNKNOWN')),  # Ensure string
                        str(item.get('transaction_sending_wallet_address', 'UNKNOWN')),  # Ensure string
                        str(item.get('transaction_receiving_wallet_address', 'UNKNOWN')),  # Ensure string
                        int(item.get("transaction_amount_send", 0)),  # Ensure int
                        str(item.get("transaction_type", "UNKNOWN")),  # Ensure string
                        int(item.get("transaction_block_number", 0)),  # Ensure int
                        item.get("transaction_found", "UNKNOWN"),
                        item.get("transaction_result", "UNKNOWN"),
                        item.get("spider_name", "UNKNOWN"),
                        item.get("spider_version", "UNKNOWN"),
                        item.get("master_addr_type", "UNKNOWN"),
                        str(item.get("master_addr_data", "UNKNOWN")),  # Ensure string
                        int(item.get("master_addr_fee", 0)),  # Ensure int
                        item.get("public_key_type", "UNKNOWN"),
                        item.get("public_key_data", "UNKNOWN"),
                        item.get("signature_type", "UNKNOWN"),
                        item.get("transaction_nonce", "UNKNOWN"),
                        item.get("transaction_addrs_to_type", "UNKNOWN"),
                        convert_timestamp_to_datetime,
                        datetimeNow,
                    )
                )
                self.connection.commit()
                logging.warning('Got new transaction, hash: %s ' % item['transaction_hash'])

            else:
                raise DropItem(f"Already got transaction: {item['transaction_hash']}")
            
        except DropItem as duplicate :
            logging.info(duplicate)
            
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item.get("item_url", "N/A"))

            self.connection.rollback()

        return item





class QrlnetworkPipeline_address:
    def open_spider(self, spider):
        """Establish a fresh database connection for this pipeline."""
        self.connection, self.cur = get_db_connection()

    def close_spider(self, spider):
        """Close database connection when the spider stops."""
        if not self.cur.closed:
            self.cur.close()
        if not self.connection.closed:
            self.connection.close()
    
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkAddressItem):
            return item
        if self.cur.closed:
            self.connection, self.cur = get_db_connection()
        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]

        if missing_fields:
            logging.warning(f"Address {item.get('wallet_address', 'UNKNOWN')} is missing fields: {missing_fields}")
            for field in missing_fields:
                item[field] = "MISSING"  # ✅ Mark missing fields but still save the item
        
        try:
            datetimeNow = datetime.now(timezone.utc)
            self.cur.execute('SELECT "wallet_address" FROM public."qrl_wallet_address" WHERE "wallet_address" = %s', (item['wallet_address'],))
            result = self.cur.fetchone()
            if result is None: 
                                
                self.cur.execute('INSERT INTO public."qrl_wallet_address" (\
                "wallet_address", "address_balance", "address_nonce",\
                "address_ots_bitfield_used_page", "address_used_ots_key_count", "address_transaction_hash_count",\
                "address_tokens_count", "address_slaves_count", "address_lattice_pk_count",\
                "address_multi_sig_address_count", "address_multi_sig_spend_count","address_inbox_message_count",\
                "address_foundation_multi_sig_spend_txn_hash", "address_foundation_multi_sig_vote_txn_hash", "address_unvotes",\
                "address_proposal_vote_stats","spider_name", "spider_version", "address_added_datetime" \
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (
                item['wallet_address'], int(item['address_balance']),int(item['address_nonce']), 
                int(item["address_ots_bitfield_used_page"]), int(item["address_used_ots_key_count"]), int(item["address_transaction_hash_count"]), 
                int(item["address_tokens_count"]) , int(item["address_slaves_count"]), int(item["address_lattice_pk_count"]), 
                int(item["address_multi_sig_address_count"]), int(item["address_multi_sig_spend_count"]),int(item["address_inbox_message_count"]), 
                item["address_foundation_multi_sig_spend_txn_hash"],item["address_foundation_multi_sig_vote_txn_hash"],item["address_unvotes"],
                item["address_proposal_vote_stats"], item["spider_name"],item["spider_version"], datetimeNow ))                
                                
                self.connection.commit()            
                logging.warning('Got New Wallet Address: %s ' % item['wallet_address'])   


            else:
        
                update_address = 'UPDATE public."qrl_wallet_address" SET "address_balance" = %s, "address_nonce" = %s ,\
                "address_ots_bitfield_used_page"= %s , "address_used_ots_key_count"= %s , "address_transaction_hash_count"= %s ,\
                "address_tokens_count"= %s , "address_slaves_count"= %s , "address_lattice_pk_count"= %s ,\
                "address_multi_sig_address_count"= %s , "address_multi_sig_spend_count"= %s ,"address_inbox_message_count"= %s ,\
                "address_foundation_multi_sig_spend_txn_hash"= %s , "address_foundation_multi_sig_vote_txn_hash"= %s , "address_unvotes"= %s ,\
                "address_proposal_vote_stats"= %s ,"spider_name"= %s , "spider_version"= %s WHERE "wallet_address" = %s'
                
                self.cur.execute(update_address,(int(item['address_balance']), int(item["address_nonce"]), int(item["address_ots_bitfield_used_page"]), 
                int(item["address_used_ots_key_count"]), int(item["address_transaction_hash_count"]), int(item["address_tokens_count"]), 
                int(item["address_slaves_count"]), int(item["address_lattice_pk_count"]), int(item["address_multi_sig_address_count"]), 
                int(item["address_multi_sig_spend_count"]),int(item["address_inbox_message_count"]), item["address_foundation_multi_sig_spend_txn_hash"],
                item["address_foundation_multi_sig_vote_txn_hash"],item["address_unvotes"],item["address_proposal_vote_stats"], 
                item["spider_name"],item["spider_version"],item["wallet_address"] ))
                    
                self.connection.commit()
                logging.info('Updated Wallet Address: %s ' % item['wallet_address'])   
                
                    
        except DropItem as duplicate :
            logging.warning(duplicate)
            
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item.get("item_url", "N/A"))

            self.connection.rollback()

        return item

        
      


class QrlnetworkPipeline_missed_items:

    def open_spider(self, spider):
        """Establish a fresh database connection for this pipeline."""
        self.connection, self.cur = get_db_connection()

    def close_spider(self, spider):
        """Close database connection when the spider stops."""
        if not self.cur.closed:
            self.cur.close()
        if not self.connection.closed:
            self.connection.close()

    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkMissedItem):
            return item
        if self.cur.closed:
            self.connection, self.cur = get_db_connection()
        # Validate item fields correctly
        if not all(field in item for field in item.fields):
            logging.error(f"Missing data in missed items: {item}")
            raise DropItem(f"Missing data in {item}")

        try:
            self.cur.execute(
                'INSERT INTO public.qrl_blockchain_missed_items ('
                '"spider_name", "spider_version", "location_script_file",'
                '"location_script_function", "trace_back", "error_type",'
                '"error_name", "item_url", "error_timestamp") '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (
                    item["spider_name"],
                    item["spider_version"],
                    item["location_script_file"],
                    item["location_script_function"],
                    json.dumps(item["trace_back"]),
                    item["error_type"],
                    item["error"],
                    item["item_url"],
                    datetime.now(timezone.utc)
                )
            )
            self.connection.commit()
            logging.warning("Got ERROR - check db")

        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item.get("item_url", "N/A"))
            self.connection.rollback()

        return item

   