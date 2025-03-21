import os
import scrapy
import re
import logging
import psycopg2
import json
import traceback
import sys
import requests
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError

from ..utils import get_db_connection, scrap_url, list_integer_to_hex
from ..items import (
    QRLNetworkBlockItem,
    QRLNetworkTransactionItem,
    QRLNetworkAddressItem,
    QRLNetworkMissedItem,
    QRLNetworkEmissionItem,
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, "Documenten")


def list_integer_to_string(data_list):
    return bytearray(data_list).decode()
logging.getLogger('scrapy.core.scraper').setLevel(logging.ERROR)

class QRLNetworkSpider(scrapy.Spider):
    name = "qrl_network_spider"
    version = "0.25"
    start_urls = ["https://zeus-proxy.automated.theqrl.org/grpc/mainnet/GetNodeState"]

    def __init__(self, retry=None, *args, **kwargs):
        super(QRLNetworkSpider, self).__init__(*args, **kwargs)
        self.retry = retry  # Activate retry mode if specified
        self.logger.info(f"Initialized spider with retry mode: {self.retry}") 
        self.connection, self.cur = get_db_connection()
        self.requested_wallets = set()  # Track wallet URLs already requested
        # Fetch & store the emission before starting scraping
        self.update_emission()
 
    def update_emission(self):
        """Fetch emission data and return it as an item."""
        try:
            self.logger.info(f"Emission updating...")
            headers = {"User-Agent": "QuantascanBot/1.0"}
            response = requests.get("https://explorer.theqrl.org/api/emission", timeout=10, headers=headers)

            if response.ok:
                emission_data = response.json()
                emission_clean = int(float(emission_data.get("emission", 0)) * 1e9)  # Convert to atomic units

                item = QRLNetworkEmissionItem()
                item["emission"] = emission_clean

                self.logger.info(f"Emission updated: {emission_clean}")
                return item  # ✅ Return the item instead of yielding

            else:
                self.logger.warning("Emission API request failed, using last stored value.")
                return None

        except Exception as e:
            self.logger.error(f"Error fetching emission: {e}")
            return None

    
    def start_requests(self):
        """
        Start requests based on mode:
            - scrapy crawl qrl_network_spider -a retry=transactions (retry failed transactions)
            - scrapy crawl qrl_network_spider -a retry=check-blocks-missing-transactions (rescrape incomplete blocks)
            - scrapy crawl qrl_network_spider -a block=12345 (rescrape a specific block)
            - scrapy crawl qrl_network_spider -a block=all (rescrape all blocks)
            - scrapy crawl qrl_network_spider -a wallet=Q01234…
            - Normal mode if no arguments are provided
        """

        #logging.getLogger('scrapy.core.scraper').setLevel(logging.INFO)
        #logging.getLogger('scrapy.core.engine').setLevel(logging.INFO)
        #logging.getLogger('scrapy.middleware').setLevel(logging.WARNING)

        if hasattr(self, "wallet") and self.wallet:
            self.logger.info(f"Searching for wallet: {self.wallet}")
            yield scrapy.Request(
                url=f"{scrap_url}/api/a/{self.wallet}",
                callback=self.parse_address,
                errback=self.errback_conn,
                meta={"wallet_search": True},
            )
            return  # Exit; only search for the specified wallet

        emission_item = self.update_emission()
        if emission_item:
            yield emission_item 

        if self.retry == "transactions":
            self.logger.info("Retry mode: Fetching failed transactions.")
            failed_urls = self.get_failed_urls()
            if not failed_urls:
                self.logger.info("No failed transactions found. Exiting retry mode.")
                return

            for url in failed_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_transaction,
                    errback=self.errback_conn,
                    meta={"retry_mode": self.retry},
                )

        elif self.retry == "check-blocks-missing-transactions":
            self.logger.info("Retry mode: Checking older blocks missing transactions.")
            # 1. Fetch blocks that are older than 2 days AND got_all_transactions = false
            blocks_to_check = self.get_blocks_older_than_two_days_not_completed()
            self.logger.info(f"Found {len(blocks_to_check)} blocks to verify.")

            for block_number, block_found_datetime, block_tx_count in blocks_to_check:
                # 2. Compare the actual transaction count from qrl_blockchain_transactions
                actual_tx_count = self.get_transaction_count_for_block(block_number)

                if actual_tx_count == block_tx_count:
                    self.logger.info(
                        f"Block {block_number} has correct number of transactions ({actual_tx_count}). "
                        "Marking as completed."
                    )
                    # 3a. Mark block as completed
                    self.mark_block_as_completed(block_number)
                else:
                    # 3b. We need to re-scrape the block
                    self.logger.info(
                        f"Block {block_number} expected {block_tx_count} but got {actual_tx_count}. Re-scraping."
                    )
                    yield scrapy.Request(
                        url=f"{scrap_url}/api/block/{block_number}",
                        callback=self.parse_block,
                        errback=self.errback_conn,
                        meta={"block_number": block_number, "retry_mode": self.retry},
                    )
        elif hasattr(self, "block"):
            if self.block.lower() == "all":
                self.logger.info("Rescraping all blocks")

                # Fetch the latest block number from the API
                self.cur.execute('SELECT MAX("block_number") FROM public."qrl_blockchain_blocks"')
                latest_block_number = self.cur.fetchall()[0][0] or 0
                if latest_block_number is None:
                    self.logger.error("Failed to get latest block number, cannot proceed with full rescrape.")
                    return

                for block_number in range(0, latest_block_number + 1):  # reversed()  # Rescrape all blocks
                    yield scrapy.Request(
                        url=f"{scrap_url}/api/block/{block_number}",
                        callback=self.parse_block,
                        errback=self.errback_conn,
                        meta={"block_number": block_number},
                    )

            elif self.block.isdigit():
                block_number = int(self.block)
                self.logger.info(f"Rescraping block {block_number}")

                yield scrapy.Request(
                    url=f"{scrap_url}/api/block/{block_number}",
                    callback=self.parse_block,
                    errback=self.errback_conn,
                    meta={"block_number": block_number},
                )

        else:
            # Normal spider behavior
            self.logger.info("Normal mode: Starting with default start_urls.")
            for url in self.start_urls:
                yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_conn)

    # -------------------------------------------------------------------------
    #                           HELPER METHODS
    # -------------------------------------------------------------------------

    def get_failed_urls(self):
        """Fetch failed transactions from the database (for retry=transactions mode)."""
        try:
            self.cur.execute(
                'SELECT item_url FROM public."qrl_blockchain_missed_items" WHERE item_url LIKE %s',
                ["%/tx/%"],
            )
            failed_urls = [row[0] for row in self.cur.fetchall()]
            self.logger.info(f"Found {len(failed_urls)} failed transactions.")
            # ✅ Log every failed transaction URL to verify structure
            for url in failed_urls:
                self.logger.info(f"Retrying failed transaction URL: {url} | Type: {type(url)}")
            return failed_urls
        except psycopg2.Error as e:
            self.logger.error(f"Database error in get_failed_urls: {e}")
            return []

    def get_blocks_older_than_two_days_not_completed(self):
        """
        Returns a list of (block_number, block_found_datetime, block_number_of_transactions)
        for blocks older than 2 days with got_all_transactions = false.
        """
        try:
            query = """
            SELECT DISTINCT ON ("block_number") "block_number", "block_found_datetime", "block_number_of_transactions"
            FROM public."qrl_blockchain_blocks"
            WHERE 
                (got_all_transactions = false OR got_all_transactions IS NULL)
                AND EXTRACT(EPOCH FROM "block_found_datetime") < (EXTRACT(EPOCH FROM NOW()) - 2*24*3600)
            ORDER BY "block_number" ASC
            """
            self.cur.execute(query)
            return self.cur.fetchall()  # list of tuples
        except psycopg2.Error as e:
            self.logger.error(f"Database error in get_blocks_older_than_two_days_not_completed: {e}")
            return []

    def get_transaction_count_for_block(self, block_number):
        """
        Returns the count of *unique* transactions found in qrl_blockchain_transactions
        for the given block_number (deduplicated by transaction_hash).
        """
        try:
            query = """
            SELECT COUNT(DISTINCT "transaction_hash")
            FROM public."qrl_blockchain_transactions"
            WHERE "transaction_block_number" = %s;
            """
            self.cur.execute(query, (block_number,))
            result = self.cur.fetchone()
            return result[0] if result else 0
        except psycopg2.Error as e:
            self.logger.error(f"Database error in get_transaction_count_for_block: {e}")
            return 0



    def mark_block_as_completed(self, block_number):
        """
        Sets got_all_transactions = true for the given block_number.
        """
        try:
            query = """
            UPDATE public."qrl_blockchain_blocks"
            SET got_all_transactions = true
            WHERE "block_number" = %s
            """
            self.cur.execute(query, (block_number,))
            self.connection.commit()
            self.logger.info(f"Block {block_number} marked got_all_transactions = true.")
        except psycopg2.Error as e:
            self.connection.rollback()
            self.logger.error(f"Database error while marking block {block_number} as complete: {e}")

    def remove_error(self, url):
        """Remove a processed error from the database (applicable for retry=transactions)."""
        try:
            self.cur.execute(
                'DELETE FROM public."qrl_blockchain_missed_items" WHERE item_url = %s',
                [url],
            )
            self.connection.commit()
            self.logger.info(f"Removed error for URL: {url}")
        except psycopg2.Error as e:
            self.logger.error(f"Database error in remove_error: {e}")
            self.connection.rollback()

    # -------------------------------------------------------------------------
    #                           SPIDER PARSE METHODS
    # -------------------------------------------------------------------------

    def parse(self, response):
        """
        Normal mode entry point: get the current block height and
        see which blocks need to be scraped (including finding gaps).
        """
        try:
            json_response = json.loads(response.body)
            current_block_height = int(json_response["info"]["block_height"])

            # Fetch the highest block number from the DB
            self.cur.execute('SELECT MAX("block_number") FROM public."qrl_blockchain_blocks"')
            highest_block_in_db = self.cur.fetchall()[0][0] or 0

            # Count how many rows are in the DB
            self.cur.execute('SELECT COUNT(*) FROM public."qrl_blockchain_blocks"')
            total_rows_in_db = self.cur.fetchall()[0][0] or 0 

            self.logger.info(
                f"Current block height: {current_block_height}, "
                f"Highest block in DB: {highest_block_in_db}, Rows in DB: {total_rows_in_db}"
            )

            # Check for large gap or any gap
            if highest_block_in_db - total_rows_in_db > 10:
                self.logger.info("Significant discrepancy found, checking for gaps...")
                self.cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" ORDER BY "block_number" ASC')
                existing_blocks = set(row[0] for row in self.cur.fetchall())

                # Generate the expected range and identify missing blocks
                expected_blocks = set(range(0, highest_block_in_db + 1))
                missing_blocks = sorted(expected_blocks - existing_blocks)
                self.logger.info(f"Gaps identified: {missing_blocks}")

                # Scrape missing blocks
                for block_number in missing_blocks:
                    self.logger.info(f"Fetching missing block number: {block_number}")
                    yield scrapy.Request(
                        url=f"{scrap_url}/api/block/{block_number}",
                        callback=self.parse_block,
                        errback=self.errback_conn,
                        meta={"block_number": block_number},
                    )

            # Scrape from highest_block_in_db+1 up to current_block_height
            for block_number in range(highest_block_in_db + 1, current_block_height + 1):
                self.logger.info(f"Fetching block number: {block_number}")
                yield scrapy.Request(
                    url=f"{scrap_url}/api/block/{block_number}",
                    callback=self.parse_block,
                    errback=self.errback_conn,
                    meta={"block_number": block_number},
                )

        except Exception as error:
            self.logger.error(f"Error in parse: {error}")
            yield self.handle_error(response, error)

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
            block_merkle_root = block_extended_header["merkle_root"]
            item_block["block_merkle_root_type"] = block_merkle_root["type"]
            item_block["block_merkle_root_data"] = list_integer_to_hex(block_merkle_root["data"])

            item_block["block_number"] = block_extended_header["block_number"]
            self.logger.info(f"block: { item_block['block_number']}")
            item_block["block_found_datetime"] = block_extended_header["timestamp_seconds"]
            item_block["block_found_timestamp_seconds"] = block_extended_header["timestamp_seconds"]

            item_block["block_reward_block"] = block_extended_header["reward_block"]
   
            reward_fee = block_extended_header.get("reward_fee", 0)

            # Ensure fee is always stored as an integer in Shor
            if isinstance(reward_fee, float):
                item_block["block_reward_fee"] = int(reward_fee * 1_000_000_000)  # Convert from Quanta to Shor
            elif isinstance(reward_fee, int):
                item_block["block_reward_fee"] = reward_fee  # Already in Shor, store as is
            else:
                item_block["block_reward_fee"] = 0

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
                    transaction_api_url = f"{scrap_url}/api/tx/{tx_hash}"

                    yield scrapy.Request(
                        url=transaction_api_url,
                        callback=self.parse_transaction,
                        errback=self.errback_conn,
                        meta={"item_block": item_block},
                    )
            else:
                self.logger.info("Block Not Found Yet By The BlockChain.")
                pass

        except Exception as error:
            self.logger.error(f"Error in parse_block: {error}")
            yield self.handle_error(response, error)

    def parse_transaction(self, response):
        item_transaction = QRLNetworkTransactionItem()

        try:
            # Ensure response.body is a valid JSON object
            json_response = response.body
            if isinstance(response.body, (str, bytes)):
                json_response = json.loads(response.body)

            if not isinstance(json_response, dict):
                self.logger.error(f"Invalid JSON format: {type(json_response)} - {json_response}")
                return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parsing error in parse_transaction: {e}")
            return None

        item_transaction["item_url"] = response.url

        try:
            # Validate transaction field
            transaction = json_response.get("transaction", {})
            if not isinstance(transaction, dict):
                self.logger.error(f"Unexpected format for transaction: {type(transaction)} - {transaction}")
                return None

            item_transaction["spider_name"] = self.name
            item_transaction["spider_version"] = self.version

            item_transaction["transaction_result"] = json_response.get("result", "Unknown")
            item_transaction["transaction_found"] = json_response.get("found", False)

            # Validate transaction_header
            transaction_header = transaction.get("header", {})
            if not isinstance(transaction_header, dict):
                self.logger.error(f"Unexpected format for transaction_header: {type(transaction_header)} - {transaction_header}")
                return None

            try:
                block_number = int(transaction_header.get("block_number", 0))
                block_found_datetime = int(transaction_header.get("timestamp_seconds", 0))
            except (TypeError, ValueError) as e:
                self.logger.error(f"Invalid data in transaction_header: {e}")
                return None

            item_transaction["transaction_block_number"] = block_number
            item_transaction["block_found_datetime"] = block_found_datetime
            item_transaction["block_found_timestamp_seconds"] = block_found_datetime

            # Validate transaction_tx
            transaction_tx = transaction.get("tx", {})
            if not isinstance(transaction_tx, dict):
                self.logger.error(f"Unexpected format for transaction_tx: {type(transaction_tx)} - {transaction_tx}")
                return None

            item_transaction["transaction_type"] = transaction_tx.get("transactionType", "Unknown")
            item_transaction["transaction_nonce"] = int(transaction_tx.get("nonce", 0))

            # Validate master_addr
            master_addr = transaction_tx.get("master_addr", {})
            if not isinstance(master_addr, dict):
                self.logger.error(f"Unexpected format for master_addr: {type(master_addr)} - {master_addr}")
                master_addr = {}

            item_transaction["master_addr_type"] = master_addr.get("type", "Unknown")
            master_addr_data = master_addr.get("data", [])
            if isinstance(master_addr_data, list):
                item_transaction["master_addr_data"] = list_integer_to_hex(master_addr_data)
            else:
                self.logger.error(f"Invalid data format for master_addr['data']: {type(master_addr_data)}")
                item_transaction["master_addr_data"] = None

            # Validate transaction hash
            transaction_hash = transaction_tx.get("transaction_hash", {})
            if isinstance(transaction_hash, str):
                item_transaction["transaction_hash"] = transaction_hash
            elif isinstance(transaction_hash, dict) and "data" in transaction_hash:
                item_transaction["transaction_hash"] = list_integer_to_hex(transaction_hash["data"])
            else:
                self.logger.error(f"Unexpected format for transaction_hash: {type(transaction_hash)} - {transaction_hash}")
                item_transaction["transaction_hash"] = None

            # Validate public_key
            public_key = transaction_tx.get("public_key", {})
            if isinstance(public_key, str):
                item_transaction["public_key_data"] = public_key
                item_transaction["public_key_type"] = "String"
            elif isinstance(public_key, dict):
                item_transaction["public_key_type"] = public_key.get("type", "Unknown")
                if isinstance(public_key.get("data"), list):
                    item_transaction["public_key_data"] = list_integer_to_hex(public_key["data"])
                else:
                    self.logger.error(f"Invalid public_key['data']: {type(public_key.get('data'))}")
                    item_transaction["public_key_data"] = None
            else:
                self.logger.error(f"Unexpected format for public_key: {type(public_key)} - {public_key}")
                item_transaction["public_key_data"] = None

            # Validate signature
            signature = transaction_tx.get("signature", {})
            if isinstance(signature, str):
                item_transaction["signature_type"] = "String"
                item_transaction["signature_data"] = signature
            elif isinstance(signature, dict):
                item_transaction["signature_type"] = signature.get("type", "Unknown")
            else:
                self.logger.error(f"Unexpected format for signature: {type(signature)} - {signature}")
                item_transaction["signature_type"] = None

            # ---------------------------
            # Handle Different Transaction Types
            # ---------------------------
            if item_transaction["transaction_type"] == "transfer":
                transaction_tx_transfer = transaction_tx.get("transfer", {})
                amounts_list = transaction_tx_transfer.get("amounts", [])
                transfer_list = []
                transfer_type_list = []

                for single_transfer in transaction_tx_transfer.get("addrs_to", []):
                    if isinstance(single_transfer, dict) and "data" in single_transfer:
                        transfer_list.append(list_integer_to_hex(single_transfer["data"]))
                        transfer_type_list.append(single_transfer.get("type", "Unknown"))

                transfer_address_amount_combined = list(zip(transfer_list, amounts_list, transfer_type_list))

                # For each recipient, create a new transaction item and yield wallet requests immediately.
                for address_with_amount in transfer_address_amount_combined:
                    local_item = item_transaction.copy()
                    sending_data = transaction.get("addr_from", {}).get("data", [])
                    local_item["transaction_sending_wallet_address"] = "Q" + list_integer_to_hex(sending_data)
                    local_item["transaction_receiving_wallet_address"] = "Q" + address_with_amount[0]
                    local_item["transaction_amount_send"] = address_with_amount[1]
                    local_item["transaction_addrs_to_type"] = address_with_amount[2]

                    yield QRLNetworkTransactionItem(local_item)

                    # Schedule wallet requests for both sending and receiving addresses,
                    # but skip the null wallet address.
                    for wallet in [local_item["transaction_receiving_wallet_address"],
                                local_item["transaction_sending_wallet_address"]]:
                        if wallet and wallet not in self.requested_wallets and wallet != "Q0000000000000000000000000000000000000000000000000000000000000000":
                            self.requested_wallets.add(wallet)
                            self.logger.info(f"Fetching wallet address details: {wallet}")
                            yield scrapy.Request(
                                url=f"{scrap_url}/api/a/{wallet}",
                                callback=self.parse_address, 
                                errback=self.errback_conn,
                                meta={"item_transaction": local_item},
                            )

            elif item_transaction["transaction_type"] == "coinbase":
                transaction_tx_coinbase = transaction_tx.get("coinbase", {})
                coinbase_transfer = transaction_tx_coinbase.get("addr_to", {})

                local_item = item_transaction.copy()
                sending_data = transaction.get("addr_from", {}).get("data", [])
                local_item["transaction_sending_wallet_address"] = "Q" + list_integer_to_hex(sending_data)
                local_item["transaction_receiving_wallet_address"] = "Q" + list_integer_to_hex(coinbase_transfer.get("data", []))
                local_item["transaction_amount_send"] = transaction_tx_coinbase.get("amount", "0")
                local_item["transaction_addrs_to_type"] = coinbase_transfer.get("type", "Unknown")

                yield QRLNetworkTransactionItem(local_item)

                for wallet in [local_item["transaction_receiving_wallet_address"],
                            local_item["transaction_sending_wallet_address"]]:
                    if wallet and wallet not in self.requested_wallets and wallet != "Q0000000000000000000000000000000000000000000000000000000000000000":
                        self.requested_wallets.add(wallet)
                        self.logger.info(f"Fetching wallet address details: {wallet}")
                        yield scrapy.Request(
                            url=f"{scrap_url}/api/a/{wallet}",
                            callback=self.parse_address,
                            errback=self.errback_conn,
                            meta={"item_transaction": local_item},
                        )

            elif item_transaction["transaction_type"] == "slave":
                transaction_tx_slave = transaction_tx.get("slave", {})
                master_data = transaction.get("addr_from", {}).get("data", [])
                if isinstance(master_data, list) and len(master_data) == 32:
                    master_address = "Q" + list_integer_to_hex(master_data)
                else:
                    master_address = ""
                for slave_pk in transaction_tx_slave.get("slave_pks", []):
                    local_item = item_transaction.copy()
                    if isinstance(slave_pk, dict):
                        slave_data = slave_pk.get("data", [])
                    elif isinstance(slave_pk, list):
                        slave_data = slave_pk
                    elif isinstance(slave_pk, str):
                        slave_address = slave_pk
                        local_item["transaction_sending_wallet_address"] = master_address
                        local_item["transaction_receiving_wallet_address"] = slave_address
                        local_item["transaction_amount_send"] = 0
                        local_item["transaction_addrs_to_type"] = ""
                        yield QRLNetworkTransactionItem(local_item.copy())
                        continue
                    else:
                        slave_data = []
                    if isinstance(slave_data, list) and len(slave_data) == 32:
                        slave_address = "Q" + list_integer_to_hex(slave_data)
                    else:
                        slave_address = ""
                    local_item["transaction_sending_wallet_address"] = master_address
                    local_item["transaction_receiving_wallet_address"] = slave_address
                    local_item["transaction_amount_send"] = 0
                    local_item["transaction_addrs_to_type"] = ""
                    yield QRLNetworkTransactionItem(local_item.copy())

                    for wallet in [master_address, slave_address]:
                        if wallet and wallet not in self.requested_wallets and wallet != "Q0000000000000000000000000000000000000000000000000000000000000000":
                            self.requested_wallets.add(wallet)
                            self.logger.info(f"Fetching wallet address details: {wallet}")
                            yield scrapy.Request(
                                url=f"{scrap_url}/api/a/{wallet}",
                                callback=self.parse_address,
                                errback=self.errback_conn,
                                meta={"item_transaction": local_item},
                            )

            elif item_transaction["transaction_type"] == "token":
                token_data = transaction_tx.get("token", {})
                initial_balances = token_data.get("initialBalances") or token_data.get("initial_balances", [])
                receiving_addresses = []
                for balance_entry in initial_balances:
                    if isinstance(balance_entry, dict):
                        address_field = balance_entry.get("address")
                        if isinstance(address_field, dict):
                            data = address_field.get("data", [])
                            if data:
                                receiving_addresses.append("Q" + list_integer_to_hex(data))
                        elif isinstance(address_field, str):
                            receiving_addresses.append(address_field)
                local_item = item_transaction.copy()
                local_item["transaction_receiving_wallet_address"] = (
                    ", ".join(receiving_addresses) if receiving_addresses else "UNKNOWN"
                )
                local_item["initial_balance_address"] = (receiving_addresses[0] if receiving_addresses else "UNKNOWN")
                local_item["initial_balance"] = (initial_balances[0].get("amount", "0")
                                                if initial_balances and isinstance(initial_balances[0], dict)
                                                else "0")
                token_symbol = token_data.get("symbol")
                if isinstance(token_symbol, dict):
                    token_symbol = list_integer_to_string(token_symbol.get("data", []))
                local_item["token_symbol"] = token_symbol or "UNKNOWN"
                token_name = token_data.get("name")
                if isinstance(token_name, dict):
                    token_name = list_integer_to_string(token_name.get("data", []))
                local_item["token_name"] = token_name or "UNKNOWN"
                token_owner = token_data.get("owner", {})
                if isinstance(token_owner, dict):
                    owner_data = token_owner.get("data", [])
                    local_item["token_owner"] = "Q" + list_integer_to_hex(owner_data) if owner_data else "UNKNOWN"
                elif isinstance(token_owner, str):
                    local_item["token_owner"] = token_owner
                else:
                    local_item["token_owner"] = "UNKNOWN"
                try:
                    local_item["token_decimals"] = int(token_data.get("decimals", 0))
                except (TypeError, ValueError):
                    local_item["token_decimals"] = 0

                yield QRLNetworkTransactionItem(local_item)

                for wallet in [local_item.get("transaction_receiving_wallet_address"),
                            local_item.get("transaction_sending_wallet_address")]:
                    if wallet and wallet not in self.requested_wallets and wallet != "Q0000000000000000000000000000000000000000000000000000000000000000":
                        self.requested_wallets.add(wallet)
                        self.logger.info(f"Fetching wallet address details: {wallet}")
                        yield scrapy.Request(
                            url=f"{scrap_url}/api/a/{wallet}",
                            callback=self.parse_address,
                            errback=self.errback_conn,
                            meta={"item_transaction": local_item},
                        )

            # Note: We have scheduled wallet requests immediately in each branch,
            # so the final loop (which fetched both addresses) is now removed.
        except Exception as error:
            self.logger.error(f"Error in parse_transaction: {error}")
            yield self.handle_error(response, error)


    def parse_address(self, response):
        try:    
            try:
                json_response = json.loads(response.body)
            except Exception as e:
                self.logger.error(f"Error decoding JSON for {response.url}: {e}")
                return

            # Initialize the address item using the wallet address extracted from the URL.
            wallet_address = response.url.split("/")[-1]
            item_address = QRLNetworkAddressItem()
            item_address["item_url"] = response.url
            item_address["spider_name"] = self.name
            item_address["spider_version"] = self.version
            item_address["wallet_address"] = wallet_address

            # Always set these keys even if API doesn't include them:
            item_address["address_foundation_multi_sig_spend_txn_hash"] = json_response.get("state", {}).get("foundation_multi_sig_spend_txn_hash", "")
            item_address["address_foundation_multi_sig_vote_txn_hash"] = json_response.get("state", {}).get("foundation_multi_sig_vote_txn_hash", "")
            item_address["address_unvotes"] = json_response.get("state", {}).get("unvotes", "")
            item_address["address_proposal_vote_stats"] = json_response.get("state", {}).get("proposal_vote_stats", "")

            # If the API indicates the wallet is not found, still yield an item with what we have.
            if not json_response.get("found", True):
                self.logger.error(f"API reports wallet {wallet_address} as invalid: {json_response.get('message')}")
                yield item_address
                return

                        # Suppress error logging for the null wallet address.
            if wallet_address == "Q0000000000000000000000000000000000000000000000000000000000000000":
                self.logger.info(f"Skipping invalid wallet (null): {wallet_address}")
                yield item_address
                return

            # If the response is valid but missing the 'state' key, log and yield a partial item.
            if "state" not in json_response:
                self.logger.error(f"Missing 'state' in response for wallet {wallet_address}: {json_response}")
   
                yield item_address
                return

            # Process valid response.
            json_state = json_response["state"]
            item_address["address_balance"] = json_state.get("balance")
            item_address["address_nonce"] = json_state.get("nonce")
            item_address["address_ots_bitfield_used_page"] = json_state.get("ots_bitfield_used_page")
            item_address["address_used_ots_key_count"] = json_state.get("used_ots_key_count")
            item_address["address_transaction_hash_count"] = json_state.get("transaction_hash_count")
            item_address["address_tokens_count"] = json_state.get("tokens_count")
            item_address["address_slaves_count"] = json_state.get("slaves_count")
            item_address["address_lattice_pk_count"] = json_state.get("lattice_pk_count")
            item_address["address_multi_sig_address_count"] = json_state.get("multi_sig_address_count")
            item_address["address_multi_sig_spend_count"] = json_state.get("multi_sig_spend_count")
            item_address["address_inbox_message_count"] = json_state.get("inbox_message_count")
        


            yield item_address


        except Exception as error:
            self.logger.error(f"Error in parse_address: {error}")
            yield self.handle_error(response, error)


    # -------------------------------------------------------------------------
    #                           ERROR HANDLERS
    # -------------------------------------------------------------------------

    def handle_error(self, response, error):
        """General error-handling function."""
        self.logger.error(f"Error encountered: {error} at {response.url}")
        item_missed = QRLNetworkMissedItem()

        item_missed["spider_name"] = self.name
        item_missed["spider_version"] = self.version
        item_missed["location_script_file"] = str(__name__)
        item_missed["location_script_function"] = str(__class__.__name__) + (", ") + str(sys._getframe().f_code.co_name)
        item_missed["trace_back"] = traceback.format_exc(limit=None, chain=True)
        item_missed["error_type"] = str(type(error))
        item_missed["error_name"] = str(error)
        item_missed["item_url"] = response.url

        return item_missed 

    def errback_conn(self, failure):
        """Handles network-related errors and logs them into the missed items table."""
        item_missed = QRLNetworkMissedItem()

        item_missed["spider_name"] = self.name
        item_missed["spider_version"] = self.version
        item_missed["location_script_file"] = str(__name__)
        item_missed["location_script_function"] = str(__class__.__name__) + ", " + str(sys._getframe().f_code.co_name)
        item_missed["trace_back"] = repr(failure.value)
        item_missed["item_url"] = failure.request.url

        if failure.check(HttpError):
            item_missed["error_name"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.value.response)

        elif failure.check(DNSLookupError):
            item_missed["error_name"] = str(failure.__class__)
            item_missed["error_type"] = "DNS Lookup Error"

        elif failure.check(TimeoutError, TCPTimedOutError):
            item_missed["error_name"] = str(failure.__class__)
            item_missed["error_type"] = "Timeout Error"

        return item_missed 
