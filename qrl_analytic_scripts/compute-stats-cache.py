"""
Pre-computes the heavy site-statistics queries and stores results in a small
cache table that the Django views read instantly.

Run every 10 minutes via cron / Heroku Scheduler:

    python compute-stats-cache.py

The cache table is created on first run.
"""

import json
import sys
import traceback
from datetime import datetime, timezone

from settings import connection, cur


CACHE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.qrl_blockchain_stats_cache (
    cache_key   VARCHAR(64) PRIMARY KEY,
    cache_value JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def ensure_cache_table():
    cur.execute(CACHE_TABLE_DDL)
    connection.commit()


def upsert(key, value):
    cur.execute(
        """
        INSERT INTO public.qrl_blockchain_stats_cache (cache_key, cache_value, updated_at)
        VALUES (%s, %s::jsonb, %s)
        ON CONFLICT (cache_key) DO UPDATE
          SET cache_value = EXCLUDED.cache_value,
              updated_at  = EXCLUDED.updated_at
        """,
        (key, json.dumps(value), datetime.now(timezone.utc)),
    )
    connection.commit()


def compute_block_stats():
    cur.execute('SELECT COALESCE(MAX(block_number), 0) FROM public.qrl_blockchain_blocks')
    highest_block_number = cur.fetchone()[0]

    cur.execute('SELECT COUNT(*) FROM public.qrl_blockchain_blocks')
    total_rows = cur.fetchone()[0]

    adjusted_rows = total_rows - 1 if total_rows > 0 else 0
    missing_blocks = (
        highest_block_number - adjusted_rows
        if highest_block_number and adjusted_rows >= 0 else 0
    )
    compliance_percentage = (
        round((adjusted_rows / highest_block_number * 100), 2)
        if highest_block_number > 0 else 0
    )

    return {
        "highest_block_number": int(highest_block_number),
        "total_rows": int(adjusted_rows),
        "missing_blocks": int(missing_blocks),
        "compliance_percentage": float(compliance_percentage),
    }


def compute_transaction_stats():
    cur.execute(
        'SELECT COALESCE(SUM(block_number_of_transactions), 0) FROM public.qrl_blockchain_blocks'
    )
    total_transactions_in_blocks = cur.fetchone()[0]

    # The slow one. Acceptable here because we only run it every 10 min in cron.
    cur.execute(
        'SELECT COUNT(DISTINCT transaction_hash) FROM public.qrl_blockchain_transactions'
    )
    total_transactions_in_database = cur.fetchone()[0]

    missing_transactions = total_transactions_in_blocks - total_transactions_in_database
    compliance_percentage_transactions = (
        round((total_transactions_in_database / total_transactions_in_blocks) * 100, 2)
        if total_transactions_in_blocks > 0 else 0
    )

    return {
        "total_transactions_in_blocks": int(total_transactions_in_blocks),
        "total_transactions_in_database": int(total_transactions_in_database),
        "missing_transactions": int(missing_transactions),
        "compliance_percentage_transactions": float(compliance_percentage_transactions),
    }


def compute_wallet_stats():
    cur.execute('SELECT COALESCE(SUM(address_balance), 0) FROM public.qrl_wallet_address')
    total_quanta_in_wallets = cur.fetchone()[0]

    cur.execute(
        'SELECT emission FROM public.qrl_blockchain_emission ORDER BY updated_at DESC LIMIT 1'
    )
    row = cur.fetchone()
    emission = int(row[0]) if row else 0
    missing_quanta = emission - int(total_quanta_in_wallets)

    return {
        "total_quanta_in_wallets": int(total_quanta_in_wallets),
        "emission": int(emission),
        "missing_quanta": int(missing_quanta),
    }


def main():
    started = datetime.now(timezone.utc)
    print(f"[stats-cache] start {started.isoformat()}")

    try:
        ensure_cache_table()

        for key, fn in (
            ("block_stats", compute_block_stats),
            ("transaction_stats", compute_transaction_stats),
            ("wallet_stats", compute_wallet_stats),
        ):
            t0 = datetime.now(timezone.utc)
            value = fn()
            upsert(key, value)
            elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
            print(f"[stats-cache] {key}: {elapsed:.2f}s -> {value}")

        total = (datetime.now(timezone.utc) - started).total_seconds()
        print(f"[stats-cache] done in {total:.2f}s")
    except Exception:
        connection.rollback()
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
