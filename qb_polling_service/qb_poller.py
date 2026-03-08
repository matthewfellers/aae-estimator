"""
AAE ERP — QuickBooks Desktop Enterprise Polling Service
========================================================

Main entry point. Connects to QB Desktop via COM, extracts sales orders,
item receipts, and inventory items, then pushes everything to Supabase.

Usage:
    python qb_poller.py                  # Run incremental sync
    python qb_poller.py --full           # Force full sync
    python qb_poller.py --test           # Test QB connection only
    python qb_poller.py --config path    # Use custom config file

Architecture:
    [QB Desktop Enterprise] → local COM → [this script] → HTTPS → [Supabase]

The ERP server on Railway never touches QB. This script runs ONLY on the
QB server machine. If the ERP is hacked, there is no path to QuickBooks.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from qb_connection import QBConnection, test_connection
from qb_queries import (
    build_sales_order_query,
    build_item_receipt_query,
    build_item_query,
)
from qb_parsers import (
    parse_sales_orders,
    parse_item_receipts,
    parse_all_items,
    check_response_status,
)
from supabase_push import SupabasePusher
from sync_log import setup_logging

logger = logging.getLogger("qb_poller")


def load_config(config_path: str = None) -> dict:
    """Load configuration from config.json."""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")

    if not os.path.exists(config_path):
        logger.error(
            f"Config file not found: {config_path}\n"
            f"Copy config.example.json to config.json and fill in your values."
        )
        sys.exit(1)

    with open(config_path, "r") as f:
        config = json.load(f)

    # Validate required fields
    required = [
        ("quickbooks", "company_file"),
        ("supabase", "url"),
        ("supabase", "service_role_key"),
    ]
    for section, key in required:
        if not config.get(section, {}).get(key):
            logger.error(f"Missing required config: {section}.{key}")
            sys.exit(1)

    if not config.get("org_id"):
        logger.error("Missing required config: org_id")
        sys.exit(1)

    return config


def should_full_sync(config: dict) -> bool:
    """Check if today is the configured full sync day."""
    full_sync_day = config.get("sync", {}).get("full_sync_day", "sunday").lower()
    today = datetime.now().strftime("%A").lower()
    return today == full_sync_day


def run_sync(config: dict, force_full: bool = False):
    """
    Execute the QB → Supabase sync.

    Steps:
        1. Determine sync type (incremental vs full)
        2. Connect to QB Desktop
        3. Query sales orders, item receipts, items
        4. Parse XML responses
        5. Push to Supabase
        6. Log results
    """
    qb_config = config["quickbooks"]
    sb_config = config["supabase"]
    sync_config = config.get("sync", {})
    org_id = config["org_id"]

    # Initialize Supabase pusher
    pusher = SupabasePusher(
        url=sb_config["url"],
        service_role_key=sb_config["service_role_key"],
        org_id=org_id,
    )

    # Determine sync type
    is_full = force_full or should_full_sync(config)
    sync_type = "full" if is_full else "incremental"
    logger.info(f"Starting {sync_type} sync...")

    # Get last sync time for incremental
    modified_since = None
    if not is_full and sync_config.get("incremental", True):
        last_sync = pusher.get_last_sync_time()
        if last_sync:
            modified_since = datetime.fromisoformat(
                last_sync.replace("Z", "+00:00")
            )
            logger.info(f"Incremental sync since: {modified_since}")
        else:
            logger.info("No previous sync found — running full sync")
            is_full = True
            sync_type = "full"

    # Log sync start
    sync_id = pusher.log_sync_start(sync_type, qb_config["company_file"])
    records = {}
    error_msg = None

    try:
        qbxml_version = qb_config.get("qbxml_version", "16.0")

        with QBConnection(
            company_file=qb_config["company_file"],
            app_name=qb_config.get("app_name", "AAE ERP Poller"),
            app_id=qb_config.get("app_id", ""),
        ) as conn:

            # ── 1. Sales Orders ──────────────────────────────────
            logger.info("Querying sales orders...")
            so_request = build_sales_order_query(
                modified_since=modified_since,
                version=qbxml_version,
            )
            so_response = conn.execute_request(so_request)

            success, msg = check_response_status(so_response)
            if success:
                orders = parse_sales_orders(so_response)
                so_counts = pusher.push_sales_orders(orders)
                records["sales_orders"] = so_counts["headers"]
                records["sales_order_lines"] = so_counts["lines"]
            else:
                logger.error(f"Sales order query failed: {msg}")
                records["sales_orders_error"] = msg

            # ── 2. Item Receipts ─────────────────────────────────
            logger.info("Querying item receipts...")
            ir_request = build_item_receipt_query(
                modified_since=modified_since,
                version=qbxml_version,
            )
            ir_response = conn.execute_request(ir_request)

            success, msg = check_response_status(ir_response)
            if success:
                receipts = parse_item_receipts(ir_response)
                ir_counts = pusher.push_item_receipts(receipts)
                records["item_receipts"] = ir_counts["headers"]
                records["item_receipt_lines"] = ir_counts["lines"]
            else:
                logger.error(f"Item receipt query failed: {msg}")
                records["item_receipts_error"] = msg

            # ── 3. Items (Inventory/NonInventory/Service) ────────
            logger.info("Querying items...")
            item_request = build_item_query(
                modified_since=modified_since if not is_full else None,
                version=qbxml_version,
            )
            item_response = conn.execute_request(item_request)

            items = parse_all_items(item_response)
            item_count = pusher.push_items(items)
            records["items"] = item_count

        logger.info(f"Sync complete! Records: {records}")

    except ConnectionError as e:
        error_msg = str(e)
        logger.error(f"Connection error: {e}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Sync failed: {e}", exc_info=True)

    # Log sync completion
    pusher.log_sync_complete(sync_id, records, error_msg)

    if error_msg:
        logger.error(f"Sync finished with errors: {error_msg}")
        return False
    else:
        logger.info("Sync finished successfully")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="AAE ERP — QuickBooks Desktop Polling Service"
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Path to config.json (default: ./config.json)"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Force a full sync (ignore incremental setting)"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Test QB Desktop connection only (no sync)"
    )
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Setup logging
    log_config = config.get("logging", {})
    setup_logging(
        log_file=log_config.get("file", "qb_sync.log"),
        level=log_config.get("level", "INFO"),
    )

    logger.info("=" * 60)
    logger.info("AAE ERP — QuickBooks Polling Service")
    logger.info("=" * 60)

    if args.test:
        # Connection test only
        company_file = config["quickbooks"]["company_file"]
        logger.info(f"Testing connection to: {company_file}")
        success = test_connection(company_file)
        if success:
            logger.info("Connection test PASSED")
        else:
            logger.error("Connection test FAILED")
        sys.exit(0 if success else 1)

    # Run sync
    success = run_sync(config, force_full=args.full)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
