from django.db import models

### Aggregated Block Data ###
class QrlAggregatedBlockData(models.Model):
    date = models.DateTimeField(primary_key=True)
    block_number_count = models.IntegerField()
    block_reward_block_sum = models.BigIntegerField()
    block_reward_block_mean = models.BigIntegerField()
    block_reward_fee_mean = models.BigIntegerField()
    block_reward_fee_sum = models.BigIntegerField()
    block_size_mean = models.BigIntegerField()
    block_size_min = models.BigIntegerField()
    block_size_max = models.BigIntegerField()
    analyze_script_date = models.DateTimeField()
    analyze_script_name = models.CharField(max_length=100)
    analyze_script_version = models.CharField(max_length=50)
    block_timestamp_seconds_max = models.BigIntegerField(blank=True, null=True)
    block_timestamp_seconds_mean = models.BigIntegerField(blank=True, null=True)
    block_timestamp_seconds_min = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'qrl_aggregated_block_data'


### Aggregated Transaction Data ###
class QrlAggregatedTransactionData(models.Model):
    analyze_script_date = models.DateTimeField()
    analyze_script_name = models.CharField(max_length=100)
    analyze_script_version = models.CharField(max_length=50)
    date = models.DateTimeField(primary_key=True)
    transaction_fee_mean = models.BigIntegerField(blank=True, null=True)
    transaction_fee_min = models.BigIntegerField(blank=True, null=True)
    transaction_fee_max = models.BigIntegerField(blank=True, null=True)
    transaction_fee_total = models.BigIntegerField(blank=True, null=True)
    total_blocks_found = models.IntegerField()
    total_number_of_transactions = models.IntegerField()
    total_amount_transfered = models.BigIntegerField()
    transaction_type = models.CharField(max_length=50)

    class Meta:
        managed = True
        db_table = 'qrl_aggregated_transaction_data'
        unique_together = (('date', 'transaction_type'),)


### Blockchain Blocks ###
class QrlBlockchainBlocks(models.Model):
    spider_name = models.CharField(max_length=255, blank=True, null=True)
    spider_version = models.CharField(max_length=50, blank=True, null=True)
    block_number = models.BigIntegerField(primary_key=True)
    block_found = models.BooleanField()
    block_result = models.TextField()
    block_size = models.IntegerField()
    block_found_datetime = models.DateTimeField()
    block_reward_block = models.BigIntegerField()
    block_reward_fee = models.BigIntegerField()
    block_mining_nonce = models.BigIntegerField()
    block_extra_nonce = models.TextField(blank=True, null=True)
    block_number_of_transactions = models.BigIntegerField(blank=True, null=True)
    block_hash_header_type = models.CharField(max_length=100, blank=True, null=True)
    block_hash_header_data = models.TextField(blank=True, null=True)
    block_hash_header_type_prev = models.CharField(max_length=100, blank=True, null=True)
    block_hash_header_data_prev = models.TextField(blank=True, null=True)
    block_merkle_root_type = models.CharField(max_length=100, blank=True, null=True)
    block_merkle_root_data = models.TextField(blank=True, null=True)
    block_added_timestamp = models.DateTimeField()
    block_found_timestamp_seconds = models.IntegerField()
    got_all_transactions = models.BooleanField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'qrl_blockchain_blocks'


### Blockchain Missed Items ###
class QrlBlockchainMissedItems(models.Model):
    id = models.BigAutoField(primary_key=True)
    spider_name = models.CharField(max_length=100, blank=True, null=True)
    spider_version = models.CharField(max_length=50, blank=True, null=True)
    item_url = models.TextField(blank=True, null=True)
    trace_back = models.TextField(blank=True, null=True)
    error_timestamp = models.DateTimeField()
    error_name = models.CharField(max_length=255, blank=True, null=True)
    error_type = models.CharField(max_length=255, blank=True, null=True)
    location_script_file = models.CharField(max_length=255, blank=True, null=True)
    location_script_function = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'qrl_blockchain_missed_items'


### Blockchain Transactions ###
class QrlBlockchainTransactions(models.Model):
    transaction_result = models.TextField(blank=True, null=True)
    spider_name = models.CharField(max_length=255)
    spider_version = models.CharField(max_length=50)
    transaction_hash = models.CharField(primary_key=True, max_length=128)
    transaction_sending_wallet_address = models.CharField(max_length=128)
    transaction_receiving_wallet_address = models.CharField(max_length=128)
    transaction_amount_send = models.BigIntegerField()
    transaction_type = models.CharField(max_length=50)
    transaction_found = models.BooleanField()
    block_found_datetime = models.DateTimeField()
    master_addr_type = models.CharField(max_length=50, blank=True, null=True)
    master_addr_data = models.TextField(blank=True, null=True)
    master_addr_fee = models.BigIntegerField(blank=True, null=True)
    public_key_type = models.CharField(max_length=50, blank=True, null=True)
    public_key_data = models.TextField(blank=True, null=True)
    signature_type = models.CharField(max_length=50, blank=True, null=True)
    transaction_nonce = models.IntegerField(blank=True, null=True)
    transaction_addrs_to_type = models.CharField(max_length=50, blank=True, null=True)
    transaction_block_number = models.BigIntegerField()
    transaction_added_datetime = models.DateTimeField()

    # Token-related fields
    initial_balance_address = models.CharField(max_length=128, blank=True, null=True)
    initial_balance = models.BigIntegerField(blank=True, null=True)
    token_symbol = models.CharField(max_length=10, blank=True, null=True)
    token_name = models.CharField(max_length=255, blank=True, null=True)
    token_owner = models.CharField(max_length=128, blank=True, null=True)
    token_decimals = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'qrl_blockchain_transactions'
        unique_together = (('transaction_hash', 'transaction_receiving_wallet_address'),)


### Wallet Address ###
class QrlWalletAddress(models.Model):
    spider_name = models.CharField(max_length=255, blank=True, null=True)
    spider_version = models.CharField(max_length=50, blank=True, null=True)
    wallet_address = models.CharField(primary_key=True, max_length=128)
    address_nonce = models.IntegerField(blank=True, null=True)
    address_ots_bitfield_used_page = models.IntegerField(blank=True, null=True)
    address_used_ots_key_count = models.BigIntegerField(blank=True, null=True)
    address_transaction_hash_count = models.BigIntegerField(blank=True, null=True)
    address_tokens_count = models.BigIntegerField(blank=True, null=True)
    address_slaves_count = models.BigIntegerField(blank=True, null=True)
    address_lattice_pk_count = models.BigIntegerField(blank=True, null=True)
    address_multi_sig_address_count = models.BigIntegerField(blank=True, null=True)
    address_multi_sig_spend_count = models.BigIntegerField(blank=True, null=True)
    address_inbox_message_count = models.BigIntegerField(blank=True, null=True)
    address_foundation_multi_sig_spend_txn_hash = models.TextField(blank=True, null=True)
    address_foundation_multi_sig_vote_txn_hash = models.TextField(blank=True, null=True)
    address_unvotes = models.TextField(blank=True, null=True)
    address_proposal_vote_stats = models.TextField(blank=True, null=True)
    address_balance = models.BigIntegerField(blank=True, null=True)
    wallet_custom_name = models.CharField(max_length=255, blank=True, null=True)
    address_added_datetime = models.DateTimeField()
    address_first_found = models.DateTimeField(blank=True, null=True)
    address_first_found_block_num = models.BigIntegerField(blank=True, null=True)
    wallet_type = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'qrl_wallet_address'
