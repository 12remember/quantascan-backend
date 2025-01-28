import os
import scrapy
import logging
import re

import logging
import psycopg2
import json
import traceback
import sys
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError

from ..settings import connection, cur, scrap_url
from ..items import (
    QRLNetworkBlockItem,
    QRLNetworkTransactionItem,
    QRLNetworkAddressItem,
    QRLNetworkMissedItem,
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, "Documenten")


def list_integer_to_hex(list_of_ints):
    array = bytearray(list_of_ints)
    return bytearray.hex(array)


class QRLNetworkSpider(scrapy.Spider):
    name = "qrl_network_spider"
    version = "0.25"
    start_urls = ["https://zeus-proxy.automated.theqrl.org/grpc/mainnet/GetNodeState"]

    def __init__(self, retry="", *args, **kwargs):
        super(QRLNetworkSpider, self).__init__(*args, **kwargs)
        self.retry = retry  # Activate retry mode if specified

    def start_requests(self):
        """
        Start requests based on mode:
          - retry=transactions -> Retry mode for failed transactions
          - retry=check-blocks-missing-transactions -> Check for blocks that are older than 2 days & missing transactions
          - else -> Normal mode
        """
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
            cur.execute(
                'SELECT item_url FROM public."qrl_blockchain_missed_items" WHERE item_url LIKE %s',
                ["%/tx/%"],
            )
            failed_urls = [row[0] for row in cur.fetchall()]
            self.logger.info(f"Found {len(failed_urls)} failed transactions.")
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
            SELECT "block_number", "block_found_datetime", "block_number_of_transactions"
            FROM public."qrl_blockchain_blocks"
            WHERE
                got_all_transactions = false
                AND EXTRACT(EPOCH FROM "block_found_datetime") < (EXTRACT(EPOCH FROM NOW()) - 2*24*3600)
            ORDER BY "block_number" ASC
            """
            cur.execute(query)
            return cur.fetchall()  # list of tuples
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
            WHERE "transaction_block_number" = %s
            """
            cur.execute(query, (block_number,))
            return cur.fetchone()[0] or 0
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
            cur.execute(query, (block_number,))
            connection.commit()
            self.logger.info(f"Block {block_number} marked got_all_transactions = true.")
        except psycopg2.Error as e:
            connection.rollback()
            self.logger.error(f"Database error while marking block {block_number} as complete: {e}")

    def remove_error(self, url):
        """Remove a processed error from the database (applicable for retry=transactions)."""
        try:
            cur.execute(
                'DELETE FROM public."qrl_blockchain_missed_items" WHERE item_url = %s',
                [url],
            )
            connection.commit()
            self.logger.info(f"Removed error for URL: {url}")
        except psycopg2.Error as e:
            self.logger.error(f"Database error in remove_error: {e}")
            connection.rollback()

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
            cur.execute('SELECT MAX("block_number") FROM public."qrl_blockchain_blocks"')
            highest_block_in_db = cur.fetchone()[0] or 0

            # Count how many rows are in the DB
            cur.execute('SELECT COUNT(*) FROM public."qrl_blockchain_blocks"')
            total_rows_in_db = cur.fetchone()[0]

            self.logger.info(
                f"Current block height: {current_block_height}, "
                f"Highest block in DB: {highest_block_in_db}, Rows in DB: {total_rows_in_db}"
            )

            # Check for large gap or any gap
            if highest_block_in_db - total_rows_in_db > 10:
                self.logger.info("Significant discrepancy found, checking for gaps...")
                cur.execute('SELECT "block_number" FROM public."qrl_blockchain_blocks" ORDER BY "block_number" ASC')
                existing_blocks = set(row[0] for row in cur.fetchall())

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
            self.handle_error(response, error)

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
            self.handle_error(response, error)

    def parse_transaction(self, response):
        item_transaction = QRLNetworkTransactionItem()
        json_response = json.loads(response.body)
        item_transaction["item_url"] = response.url

        try:
            transaction = json_response["transaction"]

            item_transaction["spider_name"] = self.name
            item_transaction["spider_version"] = self.version

            item_transaction["transaction_result"] = json_response["result"]
            item_transaction["transaction_found"] = json_response["found"]

            # transaction > header
            transaction_header = transaction["header"]
            # Reconstruct item_block directly
            reconstructed_item_block = {
                "block_number": int(transaction_header["block_number"]),
                "block_found_datetime": int(transaction_header["timestamp_seconds"]),
                "block_found_timestamp_seconds": int(transaction_header["timestamp_seconds"]),
            }

            item_transaction["transaction_block_number"] = reconstructed_item_block["block_number"]
            item_transaction["block_found_datetime"] = reconstructed_item_block["block_found_datetime"]
            item_transaction["block_found_timestamp_seconds"] = reconstructed_item_block["block_found_timestamp_seconds"]

            # transaction > tx
            transaction_tx = transaction["tx"]
            item_transaction["transaction_type"] = transaction_tx["transactionType"]
            item_transaction["transaction_nonce"] = int(transaction_tx["nonce"])

            # transaction > tx > master_addr
            master_addr = transaction_tx["master_addr"]
            item_transaction["master_addr_type"] = master_addr["type"]
            item_transaction["master_addr_data"] = list_integer_to_hex(master_addr["data"])

            # transaction > tx > public_key
            public_key = transaction_tx["public_key"]
            item_transaction["public_key_type"] = public_key["type"]
            item_transaction["public_key_data"] = list_integer_to_hex(public_key["data"])

            # transaction > tx > signature
            signature = transaction_tx["signature"]
            item_transaction["signature_type"] = signature["type"]

            # Transfer transaction
            if transaction_tx["transactionType"] == "transfer":
                transaction_tx_transfer = transaction_tx["transfer"]
                amounts_list = transaction_tx_transfer["amounts"]

                transfer_list = []
                transfer_type_list = []

                for single_transfer in transaction_tx_transfer["addrs_to"]:
                    transfer_list.append(list_integer_to_hex(single_transfer["data"]))
                    transfer_type_list.append(single_transfer["type"])

                transfer_address_amount_combined = list(zip(transfer_list, amounts_list, transfer_type_list))

                for address_with_amount in transfer_address_amount_combined:
                    # from
                    transaction_sending_wallet_address = transaction["addr_from"]
                    item_transaction["transaction_sending_wallet_address"] = "Q" + list_integer_to_hex(
                        transaction_sending_wallet_address["data"]
                    )
                    # to
                    item_transaction["transaction_receiving_wallet_address"] = "Q" + address_with_amount[0]
                    item_transaction["transaction_amount_send"] = address_with_amount[1]
                    item_transaction["transaction_addrs_to_type"] = address_with_amount[2]

                    # transaction > tx > transaction_hash
                    transaction_tx_transaction_hash = transaction_tx["transaction_hash"]
                    item_transaction["transaction_hash"] = list_integer_to_hex(transaction_tx_transaction_hash["data"])

                    yield QRLNetworkTransactionItem(item_transaction)

                    for scrape_wallet_url in [
                        item_transaction["transaction_receiving_wallet_address"],
                        item_transaction["transaction_sending_wallet_address"],
                    ]:
                        yield scrapy.Request(
                            url=f"{scrap_url}/api/a/{scrape_wallet_url}",
                            callback=self.parse_address,
                            errback=self.errback_conn,
                            meta={"item_transaction": item_transaction},
                        )

            # Coinbase transaction
            elif transaction_tx["transactionType"] == "coinbase":
                transaction_tx_coinbase = transaction_tx["coinbase"]
                coinbase_transfer = transaction_tx_coinbase["addr_to"]

                transaction_sending_wallet_address = transaction["addr_from"]
                item_transaction["transaction_sending_wallet_address"] = "Q" + list_integer_to_hex(
                    transaction_sending_wallet_address["data"]
                )
                item_transaction["transaction_receiving_wallet_address"] = "Q" + list_integer_to_hex(
                    coinbase_transfer["data"]
                )
                item_transaction["transaction_amount_send"] = transaction_tx_coinbase["amount"]
                item_transaction["transaction_addrs_to_type"] = coinbase_transfer["type"]

                coinbase_tx_transaction_hash = transaction_tx["transaction_hash"]
                item_transaction["transaction_hash"] = list_integer_to_hex(coinbase_tx_transaction_hash["data"])

                yield QRLNetworkTransactionItem(item_transaction)

                yield scrapy.Request(
                    url=f"{scrap_url}/api/a/{item_transaction['transaction_receiving_wallet_address']}",
                    callback=self.parse_address,
                    errback=self.errback_conn,
                    meta={"item_transaction": item_transaction},
                )

            # Slave transaction
            elif transaction_tx["transactionType"] == "slave":
                transaction_tx_slave = transaction_tx["slave"]
                for slave_pk in transaction_tx_slave["slave_pks"]:
                    transaction_sending_wallet_address = transaction["addr_from"]
                    item_transaction["transaction_sending_wallet_address"] = "Q" + list_integer_to_hex(
                        transaction_sending_wallet_address["data"]
                    )
                    item_transaction["transaction_receiving_wallet_address"] = "Q" + list_integer_to_hex(
                        slave_pk["data"]
                    )
                    item_transaction["transaction_amount_send"] = 0
                    item_transaction["transaction_addrs_to_type"] = ""

                    transaction_tx_transaction_hash = transaction_tx["transaction_hash"]
                    item_transaction["transaction_hash"] = list_integer_to_hex(
                        transaction_tx_transaction_hash["data"]
                    )

                    yield QRLNetworkTransactionItem(item_transaction)

            # If we're in "retry=transactions" mode, remove the error if success
            if self.retry == "transactions":
                self.remove_error(response.url)

        except Exception as error:
            self.logger.error(f"Error in parse_transaction: {error}")
            self.handle_error(response, error)

    def parse_address(self, response):
        item_transaction = response.meta["item_transaction"]
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
            item_address["address_ots_bitfield_used_page"] = json_state.get("ots_bitfield_used_page", None)
            item_address["address_used_ots_key_count"] = json_state["used_ots_key_count"]
            item_address["address_transaction_hash_count"] = json_state["transaction_hash_count"]
            item_address["address_tokens_count"] = json_state["tokens_count"]
            item_address["address_slaves_count"] = json_state["slaves_count"]
            item_address["address_lattice_pk_count"] = json_state["lattice_pk_count"]
            item_address["address_multi_sig_address_count"] = json_state["multi_sig_address_count"]
            item_address["address_multi_sig_spend_count"] = json_state["multi_sig_spend_count"]
            item_address["address_inbox_message_count"] = json_state["inbox_message_count"]

            item_address["address_foundation_multi_sig_spend_txn_hash"] = json_state[
                "foundation_multi_sig_spend_txn_hash"
            ]
            item_address["address_foundation_multi_sig_vote_txn_hash"] = json_state[
                "foundation_multi_sig_vote_txn_hash"
            ]
            item_address["address_unvotes"] = json_state["unvotes"]
            item_address["address_proposal_vote_stats"] = json_state["proposal_vote_stats"]

            yield QRLNetworkAddressItem(item_address)

        except Exception as error:
            self.logger.error(f"Error in parse_address: {error}")
            self.handle_error(response, error)

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
        item_missed["error"] = str(error)
        item_missed["item_url"] = response.url

        yield item_missed

    def errback_conn(self, failure):
        item_missed = QRLNetworkMissedItem()

        item_missed["spider_name"] = self.name
        item_missed["spider_version"] = self.version
        item_missed["location_script_file"] = str(__name__)
        item_missed["location_script_function"] = str(__class__.__name__) + (", ") + str(sys._getframe().f_code.co_name)

        if failure.check(HttpError):
            item_missed["error"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.value.response).split(" ")
            # The failing URL:
            item_missed["item_url"] = failure.request.url
            item_missed["trace_back"] = repr(failure.value)

            yield QRLNetworkMissedItem(item_missed)

        elif failure.check(DNSLookupError):
            item_missed["error"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.request).split(" ")
            item_missed["item_url"] = failure.request.url
            item_missed["trace_back"] = repr(failure.value)

            yield QRLNetworkMissedItem(item_missed)

        elif failure.check(TimeoutError, TCPTimedOutError):
            item_missed["error"] = str(failure.__class__)
            item_missed["error_type"] = str(failure.request).split(" ")
            item_missed["item_url"] = failure.request.url
            item_missed["trace_back"] = repr(failure.value)

            yield QRLNetworkMissedItem(item_missed)
