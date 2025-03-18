import psycopg2
import psycopg2.extras
import logging
import traceback
import os
import sys
import json

from datetime import datetime, timezone
from scrapy.exceptions import DropItem

from .items import (
    QRLNetworkBlockItem,
    QRLNetworkTransactionItem,
    QRLNetworkAddressItem,
    QRLNetworkMissedItem,
    QRLNetworkEmissionItem,
)
# Zorg dat je zowel get_db_connection als db_cursor importeert.
from .utils import get_db_connection, db_cursor, list_integer_to_hex


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')


def handle_spider_error(spider, error, item, item_url="N/A"):
    """Handles all database errors and logs them into the missed items table."""
    trace_back = traceback.format_exc() if error else None
    error_type = str(type(error))
    error_message = str(error) if error else "Unknown Error"
    spider_name = spider.name
    spider_version = getattr(spider, "version", "Unknown")
    spider_class = getattr(spider, "__class__", "UnknownClass").__name__


    try:
        with db_cursor() as (conn, cur):
            cur.execute(
                'INSERT INTO public.qrl_blockchain_missed_items ('
                '"spider_name", "spider_version", "location_script_file", '
                '"location_script_function", "trace_back", "error_type", '
                '"error_name", "item_url", "error_timestamp", "failed_data") '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (
                    spider_name,
                    spider_version,
                    str(__name__)[:255],
                    spider_class[:255],
                    json.dumps(trace_back) if trace_back else None,
                    error_type[:255],
                    error_message[:255],
                    item_url,
                    datetime.now(timezone.utc),
                    json.dumps(dict(item))[:1000] if item else None,
                )
            )
            conn.commit()
    except Exception as e:
        # Als er hier een fout optreedt, log deze dan ook.
        logging.error(f"Critical error logging to missed items table: {e}")


class QrlnetworkPipeline_Emission:
    def process_item(self, item, spider):
        """Processes and stores emission data in the database."""
        if not isinstance(item, QRLNetworkEmissionItem):
            return item

        try:
            datetimeNow = datetime.now(timezone.utc)
            with db_cursor() as (conn, cur):
                # Store or update emission data
                cur.execute(
                    """
                    INSERT INTO public."qrl_blockchain_emission" ("id", "emission", "updated_at")
                    VALUES (%s, %s, %s)
                    ON CONFLICT ("id") DO UPDATE 
                    SET "emission" = EXCLUDED.emission, "updated_at" = EXCLUDED.updated_at
                    """,
                    (1, item["emission"], datetimeNow),
                )
                conn.commit()
                logging.info(f"Stored/Updated Emission: {item['emission']} at {datetimeNow}")
        
        except (Exception, psycopg2.Error) as error:
            spider.logger.error(f"Database error in QrlnetworkPipeline_Emission: {error}")
            raise DropItem(f"Error storing emission data: {error}")

        return item


class QrlnetworkPipeline_block:
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkBlockItem):
            return item

        # Markeer ontbrekende velden zodat je later weet dat er iets ontbreekt
        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]
        for field in missing_fields:
            item[field] = "MISSING"

        try:
            datetimeNow = datetime.now(timezone.utc)
            with db_cursor() as (conn, cur):
                cur.execute(
                    'SELECT "block_number" FROM public."qrl_blockchain_blocks" WHERE "block_number" = %s',
                    (int(item['block_number']),)
                )
                result = cur.fetchone()
                if result is None:
                    convert_timestamp_to_datetime = datetime.fromtimestamp(
                        int(item["block_found_datetime"])
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute(
                        'INSERT INTO public."qrl_blockchain_blocks" ('
                        '"block_number", "block_found", "block_result", '
                        '"block_found_datetime", "block_found_timestamp_seconds", "block_reward_block", "block_reward_fee", '
                        '"block_mining_nonce", "block_number_of_transactions", "spider_name", '
                        '"spider_version", "block_size", "block_hash_header_type", "block_hash_header_data", '
                        '"block_hash_header_type_prev", "block_hash_header_data_prev", "block_merkle_root_type", '
                        '"block_merkle_root_data", "block_added_timestamp"'
                        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (
                            int(item['block_number']),
                            item['block_found'],
                            item['block_result'],
                            convert_timestamp_to_datetime,
                            item['block_found_timestamp_seconds'],
                            int(item["block_reward_block"]),
                            int(item["block_reward_fee"]),
                            int(item["block_mining_nonce"]),
                            int(item["block_number_of_transactions"]),
                            item["spider_name"],
                            item["spider_version"],
                            int(item["block_size"]),
                            item["block_hash_header_type"],
                            item["block_hash_header_data"],
                            item["block_hash_header_type_prev"],
                            item["block_hash_header_data_prev"],
                            item["block_merkle_root_type"],
                            item["block_merkle_root_data"],
                            datetimeNow,
                        )
                    )
                    conn.commit()
                    logging.info('Got new block, number: %s ' % item['block_number'])
                else:
                    raise DropItem("Already Got Blocknumber: %s" % item['block_number'])
        except DropItem as duplicate:
            # Duplicate block, geen actie
            pass
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item, item.get("item_url", "N/A"))
        return item


class QrlnetworkPipeline_transaction:
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkTransactionItem):
            return item

        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]
        for field in missing_fields:
            item[field] = "MISSING"

        try:
            datetimeNow = datetime.now(timezone.utc)
            with db_cursor() as (conn, cur):
                # Controleer op duplicate transacties
                cur.execute(
                    'SELECT "transaction_hash" FROM public."qrl_blockchain_transactions" '
                    'WHERE "transaction_hash" = %s AND "transaction_receiving_wallet_address" = %s',
                    (item.get('transaction_hash'), item.get('transaction_receiving_wallet_address'))
                )
                result = cur.fetchone()
                if result is None:
                    convert_timestamp_to_datetime = datetime.fromtimestamp(
                        int(item.get("block_found_datetime", 0))
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute(
                        'INSERT INTO public."qrl_blockchain_transactions" ('
                        '"transaction_hash", "transaction_sending_wallet_address", "transaction_receiving_wallet_address", '
                        '"transaction_amount_send", "transaction_type", "transaction_block_number", '
                        '"transaction_found", "transaction_result", "spider_name", '
                        '"spider_version", "master_addr_type", "master_addr_data", '
                        '"master_addr_fee", "public_key_type", "public_key_data", '
                        '"signature_type", "transaction_nonce", "transaction_addrs_to_type", '
                        '"block_found_datetime", "transaction_added_datetime"'
                        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (
                            str(item.get('transaction_hash', 'UNKNOWN')),
                            str(item.get('transaction_sending_wallet_address', 'UNKNOWN')),
                            str(item.get('transaction_receiving_wallet_address', 'UNKNOWN')),
                            int(item.get("transaction_amount_send", 0)),
                            str(item.get("transaction_type", "UNKNOWN")),
                            int(item.get("transaction_block_number", 0)),
                            item.get("transaction_found", "UNKNOWN"),
                            item.get("transaction_result", "UNKNOWN"),
                            item.get("spider_name", "UNKNOWN"),
                            item.get("spider_version", "UNKNOWN"),
                            item.get("master_addr_type", "UNKNOWN"),
                            str(item.get("master_addr_data", "UNKNOWN")),
                            int(item.get("master_addr_fee", 0)),
                            item.get("public_key_type", "UNKNOWN"),
                            item.get("public_key_data", "UNKNOWN"),
                            item.get("signature_type", "UNKNOWN"),
                            item.get("transaction_nonce", "UNKNOWN"),
                            item.get("transaction_addrs_to_type", "UNKNOWN"),
                            convert_timestamp_to_datetime,
                            datetimeNow,
                        )
                    )
                    conn.commit()
                    logging.info('Got new transaction, hash: %s ' % item['transaction_hash'])
                else:
                    raise DropItem(f"Already got transaction: {item['transaction_hash']}")
        except DropItem as duplicate:
            pass
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item, item.get("item_url", "N/A"))
        return item


class QrlnetworkPipeline_address:
    def process_item(self, item, spider):
        if not isinstance(item, QRLNetworkAddressItem):
            return item

        missing_fields = [field for field in item.keys() if item.get(field) in [None, ""]]
        for field in missing_fields:
            item[field] = "MISSING"

        try:
            datetimeNow = datetime.now(timezone.utc)
            with db_cursor() as (conn, cur):
                cur.execute(
                    'SELECT "wallet_address" FROM public."qrl_wallet_address" WHERE "wallet_address" = %s',
                    (item['wallet_address'],)
                )
                result = cur.fetchone()
                if result is None:
                    cur.execute(
                        'INSERT INTO public."qrl_wallet_address" ('
                        '"wallet_address", "address_balance", "address_nonce", '
                        '"address_ots_bitfield_used_page", "address_used_ots_key_count", "address_transaction_hash_count", '
                        '"address_tokens_count", "address_slaves_count", "address_lattice_pk_count", '
                        '"address_multi_sig_address_count", "address_multi_sig_spend_count", "address_inbox_message_count", '
                        '"address_foundation_multi_sig_spend_txn_hash", "address_foundation_multi_sig_vote_txn_hash", "address_unvotes", '
                        '"address_proposal_vote_stats", "spider_name", "spider_version", "address_added_datetime"'
                        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (
                            item['wallet_address'],
                            int(item['address_balance']),
                            int(item['address_nonce']),
                            int(item["address_ots_bitfield_used_page"]),
                            int(item["address_used_ots_key_count"]),
                            int(item["address_transaction_hash_count"]),
                            int(item["address_tokens_count"]),
                            int(item["address_slaves_count"]),
                            int(item["address_lattice_pk_count"]),
                            int(item["address_multi_sig_address_count"]),
                            int(item["address_multi_sig_spend_count"]),
                            int(item["address_inbox_message_count"]),
                            list_integer_to_hex(item["address_foundation_multi_sig_spend_txn_hash"]) if item["address_foundation_multi_sig_spend_txn_hash"] else "",
                            list_integer_to_hex(item["address_foundation_multi_sig_vote_txn_hash"]) if item["address_foundation_multi_sig_vote_txn_hash"] else "",
                            list_integer_to_hex(item["address_unvotes"]) if item["address_unvotes"] else "",
                            list_integer_to_hex(item["address_proposal_vote_stats"]) if item["address_proposal_vote_stats"] else "",
                            item["spider_name"],
                            item["spider_version"],
                            datetimeNow,
                        )
                    )
                    conn.commit()
                    logging.info('Got New Wallet Address: %s ' % item['wallet_address'])
                else:
                    cur.execute(
                        'UPDATE public."qrl_wallet_address" SET '
                        '"address_balance" = %s, "address_nonce" = %s, '
                        '"address_ots_bitfield_used_page" = %s, "address_used_ots_key_count" = %s, '
                        '"address_transaction_hash_count" = %s, "address_tokens_count" = %s, '
                        '"address_slaves_count" = %s, "address_lattice_pk_count" = %s, '
                        '"address_multi_sig_address_count" = %s, "address_multi_sig_spend_count" = %s, '
                        '"address_inbox_message_count" = %s, '
                        '"address_foundation_multi_sig_spend_txn_hash" = %s, "address_foundation_multi_sig_vote_txn_hash" = %s, '
                        '"address_unvotes" = %s, "address_proposal_vote_stats" = %s, '
                        '"spider_name" = %s, "spider_version" = %s '
                        'WHERE "wallet_address" = %s',
                        (
                            int(item['address_balance']),
                            int(item["address_nonce"]),
                            int(item["address_ots_bitfield_used_page"]),
                            int(item["address_used_ots_key_count"]),
                            int(item["address_transaction_hash_count"]),
                            int(item["address_tokens_count"]),
                            int(item["address_slaves_count"]),
                            int(item["address_lattice_pk_count"]),
                            int(item["address_multi_sig_address_count"]),
                            int(item["address_multi_sig_spend_count"]),
                            int(item["address_inbox_message_count"]),
                            list_integer_to_hex(item["address_foundation_multi_sig_spend_txn_hash"]) if item["address_foundation_multi_sig_spend_txn_hash"] else "",
                            list_integer_to_hex(item["address_foundation_multi_sig_vote_txn_hash"]) if item["address_foundation_multi_sig_vote_txn_hash"] else "",
                            list_integer_to_hex(item["address_unvotes"]) if item["address_unvotes"] else "",
                            list_integer_to_hex(item["address_proposal_vote_stats"]) if item["address_proposal_vote_stats"] else "",
                            item["spider_name"],
                            item["spider_version"],
                            item["wallet_address"],
                        )
                    )
                    conn.commit()
                    logging.info('Updated Wallet Address: %s ' % item['wallet_address'])
        except DropItem as duplicate:
            pass
        except (Exception, psycopg2.Error) as error:
            handle_spider_error(spider, error, item, item.get("item_url", "N/A"))
        return item





class QrlnetworkPipeline_missed_items:
    def process_item(self, item, spider):
        # Only process items that are QRLNetworkMissedItem
        if not isinstance(item, QRLNetworkMissedItem):
            return item

        try:
            current_time = datetime.now(timezone.utc)
            with db_cursor() as (conn, cur):
                cur.execute(
                    'INSERT INTO public."qrl_blockchain_missed_items" ('
                    '"spider_name", "spider_version", "location_script_file", '
                    '"location_script_function", "trace_back", "error_type", '
                    '"error_name", "item_url", "error_timestamp", "failed_data"'
                    ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (
                        item.get("spider_name", "UNKNOWN"),
                        item.get("spider_version", "UNKNOWN"),
                        item.get("location_script_file", "UNKNOWN"),
                        item.get("location_script_function", "UNKNOWN"),
                        item.get("trace_back", "")[:255],
                        item.get("error_type", "")[:255],
                        item.get("error_name", "")[:255],
                        item.get("item_url", "")[:255],
                        current_time,
                        item.get("failed_data", "")[:1000],
                    )
                )
                conn.commit()
        except Exception as error:
            # If there's an error writing the missed item, log it.
            logging.error(f"Error processing missed item: {error}")
            handle_spider_error(spider, error, item, item.get("item_url", "N/A"))
        return item
