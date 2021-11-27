import os
import scrapy
import logging
import re
import psycopg2
import json
import marshal
import numpy as np
import pandas as pd
import sys
import traceback


from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


from ..items import QRLNetworkBlockItem, QRLNetworkTransactionItem, QRLNetworkAddressItem, QRLNetworkMissedItem
from ..settings import connection , cur, scrap_url


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')


def list_integer_to_hex(list):
    array = bytearray(list) # create byte array from list of integers
    return bytearray.hex(array) # hex the byte array -> result tx hash, needed for api call



class QRLNetworkSpider(scrapy.Spider):
    name = "qrl_network_spider"
    version = "0.25"
    start_urls = []

    def start_requests(self):
        self.crawler.stats.set_value("spiderName", self.name)
        self.crawler.stats.set_value("spiderVersion", self.version)
                    
        yield scrapy.Request(
            url='https://explorer.theqrl.org/api/blockheight',
            callback=self.parse,
            errback=self.errback_conn,
            #meta={"item": item},
        )



    def parse(self, response):
        json_response = json.loads(response.body)

        cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" ORDER BY "block_number" DESC LIMIT 1')     
        block_in_database = cur.fetchone()
        if block_in_database != None: 
            last_block_scraped = int(block_in_database[0]) # check latest block in data base
        else:
            last_block_scraped = 0
        
        diff_with_current_blockheight = abs(json_response["blockheight"] - last_block_scraped) # calculate difference between latest block and block in database

        if json_response["found"] == True and diff_with_current_blockheight != 0 :      
            for number in range(last_block_scraped+1,json_response["blockheight"]+1 ): #last_block_scraped,json_response["blockheight"]+1
            #cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" ORDER BY "block_number" ASC') 
            #listA = [item[0] for item in cur.fetchall()]
            #res = [x for x in range(listA[0], listA[-1]+1) if x not in listA]
            #print(res)
            #for number in res:
            #for number in range(1212497,1263495):
                block_api_url = scrap_url + '/api/block/' + str(number)
                yield scrapy.Request(
                    url=block_api_url,
                    callback=self.parse_block,
                    errback=self.errback_conn,
                    #meta={"item": item},
                )


    def parse_block(self, response):
        item_block = QRLNetworkBlockItem()
        json_response = json.loads(response.body)
        item_block["item_url"] = response.url   
        try:
            item_block["spider_name"] = self.name
            item_block["spider_version"] = self.version
               
            item_block["block_result"] = json_response["result"]
            item_block["block_found"] = json_response["found"]
            
            # block_extended
            block_extended = json_response["block_extended"]  
            item_block["block_size"] = block_extended["size"]
                
            
            # block_extended >  header
            block_extended_header = block_extended["header"]
            
            # block_extended >  header > hash_header
            block_hash_header = block_extended_header["hash_header"]
            item_block["block_hash_header_type"] = block_hash_header["type"]
            item_block["block_hash_header_data"] = list_integer_to_hex(block_hash_header["data"])
            
            # block_extended >  header > hash_header_prev
            block_hash_header_prev = block_extended_header["hash_header_prev"]
            item_block["block_hash_header_type_prev"] = block_hash_header_prev["type"]
            item_block["block_hash_header_data_prev"] = list_integer_to_hex(block_hash_header_prev["data"])
            
            # block_extended >  header > merkle_root    
            block_merkle_root= block_extended_header["merkle_root"]
            item_block["block_merkle_root_type"] = block_merkle_root["type"]
            item_block["block_merkle_root_data"] = list_integer_to_hex(block_merkle_root["data"])
                
            item_block["block_number"] = block_extended_header["block_number"]
            item_block["block_found_datetime"] = block_extended_header["timestamp_seconds"]
            item_block["block_found_timestamp_seconds"] = block_extended_header["timestamp_seconds"]

            item_block["block_reward_block"] = block_extended_header["reward_block"]
            item_block["block_reward_fee"] = block_extended_header["reward_fee"]
            item_block["block_mining_nonce"] = block_extended_header["mining_nonce"]
            item_block["block_extra_nonce"] = block_extended_header["extra_nonce"]
            
            item_block["block_number_of_transactions"] = len(block_extended["extended_transactions"])

            if item_block["block_found"] == True:
                yield QRLNetworkBlockItem(item_block)

                for transaction in block_extended["extended_transactions"]:
                    transaction_tx = transaction["tx"]
                    transaction_tx_transaction_hash = transaction_tx["transaction_hash"]
                    tx_hash = list_integer_to_hex(transaction_tx_transaction_hash["data"])

                    # create api url
                    transaction_api_url = scrap_url + '/api/tx/' + str(tx_hash)
                    
                    yield scrapy.Request(
                        url=transaction_api_url,
                        callback=self.parse_transaction,
                        errback=self.errback_conn,
                        meta={"item_block": item_block},
                    )
            else:
                print('Block Not Found Yet By The BlockChain')
                pass
       
        except (Exception) as error:
            item_missed = QRLNetworkMissedItem()
            
            item_missed["spider_name"] = self.name
            item_missed["spider_version"] = self.version
            item_missed["location_script_file"] = str(__name__)
            item_missed["location_script_function"] = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
            item_missed["trace_back"] = traceback.format_exc(limit=None, chain=True)
            item_missed["error_type"] = str(type(error))
            item_missed["error"] = str(error)
            item_missed["item_url"] = response.url

            yield QRLNetworkMissedItem(item_missed)   
       
            
            
    def parse_transaction(self, response):
        item_block = response.meta['item_block']
        item_transaction = QRLNetworkTransactionItem()
        json_response = json.loads(response.body)
        item_transaction["item_url"]=response.url
        
        try:
            transaction = json_response["transaction"]
                    
            item_transaction["spider_name"] = self.name
            item_transaction["spider_version"] = self.version
                    
            item_transaction["transaction_result"] = json_response["result"]
            item_transaction["transaction_found"] = json_response["found"]
            
            # transaction >  header 
            transaction_header = transaction["header"]
            item_transaction["transaction_block_number"] = item_block["block_number"]
            item_transaction["block_found_datetime"] = item_block["block_found_datetime"]
            item_transaction["block_found_timestamp_seconds"] = item_block["block_found_timestamp_seconds"]
            
            # transaction >  tx  
            transaction_tx = transaction["tx"]
            item_transaction["transaction_type"] = transaction_tx["transactionType"]
            item_transaction["transaction_nonce"] = int(transaction_tx["nonce"])
            item_transaction["master_addr_fee"]  =  int(transaction_tx["fee"])
            
            # transaction >  tx  > master_addr
            master_addr = transaction_tx["master_addr"]
            item_transaction["master_addr_type"] = master_addr["type"]
            item_transaction["master_addr_data"]  = list_integer_to_hex(master_addr["data"])
            
            
            # transaction >  tx  > public_key
            public_key = transaction_tx["public_key"]
            item_transaction["public_key_type"] = public_key["type"]
            item_transaction["public_key_data"]  = list_integer_to_hex(public_key["data"])
          
            # transaction >  tx  > signature 
            signature = transaction_tx["signature"]
            item_transaction["signature_type"] = signature["type"]
            item_transaction["signature_data"]  = list_integer_to_hex(signature["data"])

            
            
            if transaction_tx["transactionType"] == "transfer":
            
                transfer_list = []
                transfer_type_list = []
                transaction_tx_transfer = transaction_tx["transfer"]
                amounts_list = transaction_tx_transfer["amounts"]    
                
                # transaction >  tx  > transfer > addrs_to    
                for single_transfer in transaction_tx_transfer["addrs_to"]:
                    transaction_addrs_to_type = single_transfer["type"]
                    transaction_receiving_wallet_address_hex = list_integer_to_hex(single_transfer["data"]) # get receiving address in hex 
                    transfer_type_list.append(transaction_addrs_to_type)       
                    transfer_list.append(transaction_receiving_wallet_address_hex)
                
                transfer_address_amount_combined = list(zip(transfer_list, amounts_list,transfer_type_list))     
                
                # transaction >  tx  > transfer > amounts
                for address_with_amount in transfer_address_amount_combined:
                    transaction_sending_wallet_address = transaction["addr_from"]
                    item_transaction["transaction_sending_wallet_address"] = "Q"+ list_integer_to_hex(transaction_sending_wallet_address["data"])
                    item_transaction["transaction_receiving_wallet_address"] = "Q" + address_with_amount[0]
                    item_transaction["transaction_amount_send"] = address_with_amount[1]
                    item_transaction["transaction_addrs_to_type"] = address_with_amount[2]          
                    
                    # transaction >  tx  > transaction_hash
                    transaction_tx_transaction_hash = transaction_tx["transaction_hash"]
                    item_transaction["transaction_hash"] = list_integer_to_hex(transaction_tx_transaction_hash["data"])

                    
                    yield QRLNetworkTransactionItem(item_transaction)
                                    
                    for scrape_wallet_url in [item_transaction["transaction_receiving_wallet_address"] ,item_transaction["transaction_sending_wallet_address"], ] :
                        yield scrapy.Request(
                            url= scrap_url + "/api/a/" + scrape_wallet_url,
                            callback=self.parse_address,
                            errback=self.errback_conn,
                            meta={"item_transaction": item_transaction,}
                        )    
             
                   
                        
            elif transaction_tx["transactionType"] == "coinbase":
                # transaction >  tx  > coinbase
                transaction_tx_coinbase = transaction_tx["coinbase"]
                coinbase_transfer = transaction_tx_coinbase["addr_to"]

                transaction_sending_wallet_address = transaction["addr_from"]
                
                item_transaction["transaction_sending_wallet_address"] = "Q"+ list_integer_to_hex(transaction_sending_wallet_address["data"])
                item_transaction["transaction_receiving_wallet_address"] = "Q" + list_integer_to_hex(coinbase_transfer["data"])
                item_transaction["transaction_amount_send"] = transaction_tx_coinbase["amount"]
                item_transaction["transaction_addrs_to_type"] = coinbase_transfer["type"]

            
                # transaction >  tx  > transaction_hash
                coinbase_tx_transaction_hash = transaction_tx["transaction_hash"]
                item_transaction["transaction_hash"] =  list_integer_to_hex(coinbase_tx_transaction_hash["data"])
                
                yield QRLNetworkTransactionItem(item_transaction)
                
                                            
                yield scrapy.Request(
                    url= scrap_url + "/api/a/" + item_transaction["transaction_receiving_wallet_address"],
                    callback=self.parse_address,
                    errback=self.errback_conn,
                    meta={"item_transaction": item_transaction,},
                )  
                
            elif transaction_tx["transactionType"] == "slave" :

                # transaction >  tx  > slave
                transaction_tx_slave = transaction_tx["slave"]
        
                for slave_pk in transaction_tx_slave["slave_pks"] :
                    transaction_sending_wallet_address = transaction["addr_from"]
                    item_transaction["transaction_sending_wallet_address"] = "Q"+ list_integer_to_hex(transaction_sending_wallet_address["data"])
                    item_transaction["transaction_receiving_wallet_address"] = "Q" + list_integer_to_hex(slave_pk["data"])                    
                    item_transaction["transaction_amount_send"] = 0 #address_with_access_types[1]
                    item_transaction["transaction_addrs_to_type"] = '' #address_with_access_types[2]
     
                    
                    transaction_tx_transaction_hash = transaction_tx["transaction_hash"]
                    item_transaction["transaction_hash"] = list_integer_to_hex(transaction_tx_transaction_hash["data"])

                    yield QRLNetworkTransactionItem(item_transaction)
       
                                            
            
        except (Exception) as error:        
            item_missed = QRLNetworkMissedItem()
            
            item_missed["spider_name"] = self.name
            item_missed["spider_version"] = self.version
            item_missed["location_script_file"] = str(__name__)
            item_missed["location_script_function"] = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
            item_missed["trace_back"] = traceback.format_exc(limit=None, chain=True)
            item_missed["error_type"] = str(type(error))
            item_missed["error"] = str(error)
            item_missed["item_url"] = response.url

            yield QRLNetworkMissedItem(item_missed)   
            

    def parse_address(self, response):
        item_transaction = response.meta['item_transaction']     
        item_address = QRLNetworkAddressItem()
        json_response = json.loads(response.body)
        item_address["item_url"] = response.url
    
            
        try:
            json_state = json_response["state"]

            item_address["spider_name"] = self.name
            item_address["spider_version"] = self.version
            
            item_address["wallet_address"] = json_state["address"]
            item_address["address_balance"] = json_state["balance"]
            item_address["address_nonce"] = json_state["nonce"]
            item_address["address_ots_bitfield_used_page"] = json_state["ots_bitfield_used_page"]
            item_address["address_used_ots_key_count"] = json_state["used_ots_key_count"]
            item_address["address_transaction_hash_count"] = json_state["transaction_hash_count"]
            item_address["address_tokens_count"] = json_state["tokens_count"]
            item_address["address_slaves_count"] = json_state["slaves_count"]
            item_address["address_lattice_pk_count"] = json_state["lattice_pk_count"]
            item_address["address_multi_sig_address_count"] = json_state["multi_sig_address_count"]
            item_address["address_multi_sig_spend_count"] = json_state["multi_sig_spend_count"]
            item_address["address_inbox_message_count"] = json_state["inbox_message_count"]
                        
            item_address["address_foundation_multi_sig_spend_txn_hash"] = json_state["foundation_multi_sig_spend_txn_hash"]
            item_address["address_foundation_multi_sig_vote_txn_hash"] = json_state["foundation_multi_sig_vote_txn_hash"]
            item_address["address_unvotes"] = json_state["unvotes"]
            item_address["address_proposal_vote_stats"] = json_state["proposal_vote_stats"]
            
            item_address["address_proposal_vote_stats"] = json_state["proposal_vote_stats"]

                
            yield QRLNetworkAddressItem(item_address)



        except (Exception) as error:
            item_missed = QRLNetworkMissedItem()
            
            item_missed["spider_name"] = self.name
            item_missed["spider_version"] = self.version
            item_missed["location_script_file"] = str(__name__)
            item_missed["location_script_function"] = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
            item_missed["trace_back"] = traceback.format_exc(limit=None, chain=True)
            item_missed["error_type"] = str(type(error))
            item_missed["error"] = str(error)
            item_missed["item_url"] = response.url

            yield QRLNetworkMissedItem(item_missed)         
            
            
    def errback_conn(self, failure):
        item_missed = QRLNetworkMissedItem()
                        
        item_missed["spider_name"] = self.name
        item_missed["spider_version"] = self.version
        item_missed["location_script_file"] = str(__name__)
        item_missed["location_script_function"] = str(__class__.__name__) + (', ') + str(sys._getframe().f_code.co_name)
        
        if failure.check(HttpError):

            item_missed["error"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.value.response).split(" ")
            item_missed["item_url"] = failVal[1]
            item_missed["trace_back"] = failVal[0]

            yield QRLNetworkMissedItem(item_missed)
            
        elif failure.check(DNSLookupError):

            item_missed["error"] = str(failure.__class__)
            item_missed["errorType"] = str(failure.request).split(" ")
            item_missed["item_url"] = failVal[1]
            item_missed["trace_back"] = failVal[0]
            
            yield QRLNetworkMissedItem(item_missed)

        elif failure.check(TimeoutError, TCPTimedOutError):
            item_missed["error"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.request).split(" ")
            item_missed["item_url"] = failVal[1]
            item_missed["trace_back"] = failVal[0]      
            
            yield QRLNetworkMissedItem(item_missed)
        
                  
#def spiderError(missedIn,itemError, itemErrorType, fileName,itemUrl, missedItemType):
#    cur = connection.cursor()
#    error_timestamp = datetime.now()
    
#    try:
#        cur.execute('INSERT INTO public. "qrl_blockchain_missed_items" (\
#        "spider_name","spider_version", "missed_in",\
#        "item_error", "item_error_type", "file_name", "item_url",\
#        "missed_item_type", "error_timestamp"\
#        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s, %s)', (
#        QRLNetworkSpider.name,QRLNetworkself.version,missedIn,itemError,itemErrorType,fileName, itemUrl, missedItemType,error_timestamp))

#        connection.commit()
#        logging.warning('Got ERROR - check db')

#    except Exception as error:
#        connection.rollback()        
            
       
# self.crawler.engine.close_spider(self, 'log message')


