import scrapy


class QRLNetworkBlockItem(scrapy.Item):
    spider_name = scrapy.Field()
    spider_version = scrapy.Field()
    
    block_result=scrapy.Field()
    block_found=scrapy.Field()
    block_number=scrapy.Field()
    block_size =scrapy.Field() 
    block_found_datetime=scrapy.Field()
    block_found_timestamp_seconds=scrapy.Field()
    block_reward_block=scrapy.Field()
    block_reward_fee=scrapy.Field()
    block_mining_nonce=scrapy.Field()
    block_extra_nonce=scrapy.Field()
    block_number_of_transactions=scrapy.Field()
    
    block_hash_header_type =scrapy.Field()
    block_hash_header_data =scrapy.Field() 
    block_hash_header_type_prev =scrapy.Field()
    block_hash_header_data_prev =scrapy.Field()  
    block_merkle_root_type =scrapy.Field()  
    block_merkle_root_data =scrapy.Field()    
    
    item_url =scrapy.Field() 
    pass


class QRLNetworkTransactionItem(scrapy.Item):
    spider_name = scrapy.Field()
    spider_version = scrapy.Field()
    transaction_result = scrapy.Field()
    transaction_found = scrapy.Field()
    transaction_block_number = scrapy.Field()
    transaction_type = scrapy.Field()
    
    transaction_receiving_wallet_address = scrapy.Field()
    transaction_sending_wallet_address = scrapy.Field()
    transaction_amount_send = scrapy.Field()
    transaction_hash = scrapy.Field()
    
    block_found_datetime = scrapy.Field()
    block_found_timestamp_seconds = scrapy.Field()    
    
    master_addr_type = scrapy.Field()
    master_addr_data = scrapy.Field()
    master_addr_fee = scrapy.Field()
    public_key_type = scrapy.Field()
    public_key_data = scrapy.Field()
    signature_data = scrapy.Field()
    signature_type = scrapy.Field()
    transaction_nonce = scrapy.Field()
    transaction_addrs_to_type = scrapy.Field()
    initial_balance = scrapy.Field()
    initial_balance_address = scrapy.Field()
    token_symbol = scrapy.Field()
    token_name = scrapy.Field()
    token_owner = scrapy.Field()
    token_decimals = scrapy.Field()


    item_url =scrapy.Field()           
    pass

class QRLNetworkAddressItem(scrapy.Item):
    spider_name = scrapy.Field()
    spider_version = scrapy.Field()
    
    wallet_address = scrapy.Field()
    address_balance = scrapy.Field()
    address_nonce = scrapy.Field()
    address_ots_bitfield_used_page = scrapy.Field()
    address_used_ots_key_count = scrapy.Field()
    address_transaction_hash_count = scrapy.Field()
    address_tokens_count = scrapy.Field()
    address_slaves_count = scrapy.Field()
    address_lattice_pk_count = scrapy.Field()
    address_multi_sig_address_count = scrapy.Field()
    address_multi_sig_spend_count = scrapy.Field()
    address_inbox_message_count = scrapy.Field()
    address_foundation_multi_sig_spend_txn_hash =  scrapy.Field()
    address_foundation_multi_sig_vote_txn_hash = scrapy.Field() 
    address_unvotes = scrapy.Field()
    address_proposal_vote_stats =  scrapy.Field()
    
    address_last_updated =  scrapy.Field()
    item_url =scrapy.Field()     
    pass


class QRLNetworkMissedItem(scrapy.Item):
    spider_name = scrapy.Field()
    spider_version = scrapy.Field()
    location_script_file = scrapy.Field()
    location_script_function = scrapy.Field()    
    trace_back = scrapy.Field()
    error_type = scrapy.Field()
    error_name = scrapy.Field()
    item_url = scrapy.Field()
    error_timestamp = scrapy.Field()
    failed_data = scrapy.Field()
    
        
    pass        


class QRLNetworkMissedItem(scrapy.Item):
    spider_name = scrapy.Field()
    spider_version = scrapy.Field()
    location_script_file = scrapy.Field()
    location_script_function = scrapy.Field()    
    trace_back = scrapy.Field()
    error_type = scrapy.Field()
    error_name = scrapy.Field()
    item_url = scrapy.Field()
    error_timestamp = scrapy.Field()
    failed_data = scrapy.Field()
    
        
    pass        


class QRLNetworkEmissionItem(scrapy.Item):
    emission = scrapy.Field()
    updated_at = scrapy.Field()
