import psycopg2
import psycopg2.extras
import logging
import traceback
import sched, time
import os
import sys
import json

from scrapy import signals
from datetime import datetime
from django.utils import timezone
from psycopg2.extensions import AsIs
from psycopg2.extras import LoggingConnection, LoggingCursor

#from scrapy.conf import settings
from scrapy.exceptions import DropItem
 
from .items import QRLNetworkBlockItem, QRLNetworkTransactionItem, QRLNetworkAddressItem, QRLNetworkMissedItem
from .settings import connection , cur, scrap_url 

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')


class QrlnetworkPipeline_block:
    def open_spider(self, spider):        
        cur = connection.cursor()


    def close_spider(self, spider):
        cur.close()
        connection.close()

        
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkBlockItem):
            return item
            
        valid = True
        for data in item: 
            if not data:
                valid = False
                logging.error('Missing data in block', data)
                raise DropItem("Missing data in block {0}!".format(data))
              
        if valid:
            try:
                datetimeNow = datetime.now()
                cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" WHERE "block_number" = %s', (int(item['block_number']),))
                dup_check = len(cur.fetchall())
                if dup_check == 0: 
                    convert_timestamp_to_datetime = datetime.fromtimestamp(int(item["block_found_datetime"])).strftime("%Y-%m-%d %H:%M:%S")  
                    cur.execute('INSERT INTO public. "qrl_blockchain_blocks" (\
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
                    connection.commit()
                    logging.warning('Got new block, number: %s ' % item['block_number'])
                else:
                    raise DropItem("Already Got Blocknumber: %s" % item['block_number'])

            except DropItem as duplicate :
                logging.info(duplicate)
                
            except (Exception, psycopg2.Error) as error:
                spider_name = spider.name,
                spider_version = spider.version,
                location_script_file = str(__name__)
                location_script_function = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
                trace_back = traceback.format_exc(limit=None, chain=True)
                error_type = str(type(error))
                error = str(error)
                item_url = item["item_url"]

                spiderError(spider_name, spider_version, location_script_file, location_script_function, trace_back, error_type, error, item_url)
                connection.rollback()

        return item

     

class QrlnetworkPipeline_transaction:
    def open_spider(self, spider):
        cur = connection.cursor()


    def close_spider(self, spider):
        cur.close()
        connection.close()

            
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkTransactionItem):
            return item
        
        valid = True
        for data in item: 
            if not data:
                valid = False
                logging.error('Missing data in transaction', data)
                raise DropItem("Missing data in transaction {0}!".format(data))                
                                      
        if valid:
            try:
                datetimeNow = datetime.now()
                cur.execute('SELECT "transaction_hash" FROM public."qrl_blockchain_transactions" WHERE "transaction_hash" = %s AND "transaction_receiving_wallet_address" = %s', (item['transaction_hash'], item['transaction_receiving_wallet_address']))
                dup_check = len(cur.fetchall())
                if dup_check == 0: 
                    convert_timestamp_to_datetime = datetime.fromtimestamp(int(item["block_found_datetime"])).strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute('INSERT INTO public. "qrl_blockchain_transactions" (\
                    "transaction_hash", "transaction_sending_wallet_address", "transaction_receiving_wallet_address",\
                    "transaction_amount_send", "transaction_type", "transaction_block_number",\
                    "transaction_found", "transaction_result","spider_name", \
                    "spider_version" , "master_addr_type", "master_addr_data",\
                    "master_addr_fee","public_key_type","public_key_data",\
                    "signature_type","transaction_nonce",\
                    "transaction_addrs_to_type", "block_found_datetime",\
                    "transaction_added_datetime" \
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                    (item['transaction_hash'], item['transaction_sending_wallet_address'],item['transaction_receiving_wallet_address'], 
                    int(item["transaction_amount_send"]), item["transaction_type"], int(item["transaction_block_number"]), 
                    item["transaction_found"],item["transaction_result"], item["spider_name"],
                    item["spider_version"],item["master_addr_type"],item["master_addr_data"],
                    item["master_addr_fee"], item["public_key_type"], item["public_key_data"], 
                    item["signature_type"], item["transaction_nonce"],
                    item["transaction_addrs_to_type"], convert_timestamp_to_datetime, datetimeNow ))

                    connection.commit()
                    logging.warning('Got new transaction, hash: %s ' % item['transaction_hash'])        
                else:
                    raise DropItem("Already Got Transaction: %s" % item['transaction_hash'])


            except DropItem as duplicate :
                logging.info(duplicate)
                
            except (Exception, psycopg2.Error) as error:
                connection.rollback()
                        
                spider_name = spider.name,
                spider_version = spider.version,
                location_script_file = str(__name__)
                location_script_function = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
                trace_back = traceback.format_exc()
                error_type = str(type(error))
                error = str(error)
                item_url = item["item_url"]
                 
                spiderError(spider_name, spider_version, location_script_file, location_script_function, trace_back, error_type, error, item_url)
        
        return item





class QrlnetworkPipeline_address:
    def open_spider(self, spider):
        cur = connection.cursor()


    def close_spider(self, spider):
        cur.close()
        connection.close()

    
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkAddressItem):
            return item

        valid = True
        for data in item: 
            if not data:
                valid = False
                logging.error('Missing data in address', data)
                raise DropItem("Missing {0}!".format(data))
        if valid:
            try:
                datetimeNow = datetime.now()
                cur.execute('SELECT "wallet_address" FROM public."qrl_wallet_address" WHERE "wallet_address" = %s', (item['wallet_address'],))
                dup_check = len(cur.fetchall())
                if dup_check == 0:
                                    
                    cur.execute('INSERT INTO public. "qrl_wallet_address" (\
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
                                    
                    connection.commit()            
                    logging.warning('Got New Wallet Address: %s ' % item['wallet_address'])   


                else:
            
                    update_address = 'UPDATE public. "qrl_wallet_address" SET "address_balance" = %s, "address_nonce" = %s ,\
                    "address_ots_bitfield_used_page"= %s , "address_used_ots_key_count"= %s , "address_transaction_hash_count"= %s ,\
                    "address_tokens_count"= %s , "address_slaves_count"= %s , "address_lattice_pk_count"= %s ,\
                    "address_multi_sig_address_count"= %s , "address_multi_sig_spend_count"= %s ,"address_inbox_message_count"= %s ,\
                    "address_foundation_multi_sig_spend_txn_hash"= %s , "address_foundation_multi_sig_vote_txn_hash"= %s , "address_unvotes"= %s ,\
                    "address_proposal_vote_stats"= %s ,"spider_name"= %s , "spider_version"= %s WHERE "wallet_address" = %s'
                    
                    cur.execute(update_address,(int(item['address_balance']), int(item["address_nonce"]), int(item["address_ots_bitfield_used_page"]), 
                    int(item["address_used_ots_key_count"]), int(item["address_transaction_hash_count"]), int(item["address_tokens_count"]), 
                    int(item["address_slaves_count"]), int(item["address_lattice_pk_count"]), int(item["address_multi_sig_address_count"]), 
                    int(item["address_multi_sig_spend_count"]),int(item["address_inbox_message_count"]), item["address_foundation_multi_sig_spend_txn_hash"],
                    item["address_foundation_multi_sig_vote_txn_hash"],item["address_unvotes"],item["address_proposal_vote_stats"], 
                    item["spider_name"],item["spider_version"],item["wallet_address"] ))
                        
                    connection.commit()
                    logging.info('Updated Wallet Address: %s ' % item['wallet_address'])   
                    
                        
            except DropItem as duplicate :
                logging.warning(duplicate)
                
            except (Exception, psycopg2.Error) as error:
                connection.rollback()
                        
                spider_name = spider.name,
                spider_version = spider.version,
                location_script_file = str(__name__)
                location_script_function = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
                trace_back = traceback.format_exc()
                error_type = str(type(error))
                error = str(error)
                item_url = item["item_url"]
                 
                spiderError(spider_name, spider_version, location_script_file, location_script_function, trace_back, error_type, error, item_url)


        
        return item



class QrlnetworkPipeline_missed_items:

    def open_spider(self, spider):
        cur = connection.cursor()


    def close_spider(self, spider):
        cur.close()
        connection.close()

    
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkMissedItem):
            return item
        
        valid = True    
        for data in item: 
            if not data:
                valid = False
                logging.error('Missing data in missed items', data)
                raise DropItem("Missing {0}!".format(data))
        
        try:
            if valid:    
                cur.execute('INSERT INTO public. "qrl_blockchain_missed_items" (\
                "spider_name","spider_version", "location_script_file",\
                "location_script_function", "trace_back", "error_type",\
                "error_name", "item_url", "error_timestamp"\
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (
                item["spider_name"], item["spider_version"], item["location_script_file"], item["location_script_function"], json.dumps(item["trace_back"]), 
                item["error_type"], item["error"], item["item_url"], datetime.now()))

                connection.commit()
                logging.warning('Got ERROR - check db')
            return item
        

        except (Exception, psycopg2.Error) as error:
            connection.rollback()
                    
            spider_name = spider.name,
            spider_version = spider.version,
            location_script_file = str(__name__)
            location_script_function = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
            trace_back = traceback.format_exc()
            error_type = str(type(error))
            error = str(error)
            item_url = item["item_url"]
             
            spiderError(spider_name, spider_version, location_script_file, location_script_function, trace_back, error_type, error, item_url)



 
def spiderError(spider_name, spider_version, location_script_file, location_script_function, trace_back, error_type, error, item_url):
        
    cur = connection.cursor()
    
    try:    
        cur.execute('INSERT INTO public. "qrl_blockchain_missed_items" (\
        "spider_name","spider_version", "location_script_file",\
        "location_script_function", "trace_back", "error_type",\
        "error_name", "item_url", "error_timestamp"\
        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (
        spider_name, spider_version, location_script_file, 
        location_script_function, json.dumps(trace_back), error_type, error, item_url, datetime.now()))

        connection.commit()
        logging.warning('Got ERROR - check db')

    except (Exception,psycopg2.Error) as error:
        connection.rollback()

        location_script_file = str(__name__)
        location_script_function = str(sys._getframe().f_code.co_name) + ', exception'
        trace_back = traceback.format_exc()
        error = str(error)

        
        cur.execute('INSERT INTO public. "qrl_blockchain_missed_items" (\
        "spider_name","spider_version", "location_script_file",\
        "location_script_function", "trace_back", "error_type",\
        "error_name", "item_url", "error_timestamp"\
        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (
        '', '', location_script_file, location_script_function, 
        json.dumps(trace_back), 'Unknown Error', error, '', datetime.now()))

        connection.commit()
        logging.warning('Got Unkown ERROR - check db')

 