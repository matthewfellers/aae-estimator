"""
Supabase Push Module — upserts parsed QB data into Supabase staging tables.

Uses the service-role key (bypasses RLS) since the polling service
runs on the QB machine, not the ERP server.
"""

import logging
from typing import Any
from supabase import create_client, Client

logger = logging.getLogger("qb_poller")


class SupabasePusher:
    """Handles upserting QB data into Supabase staging tables."""

    def __init__(self, url: str, service_role_key: str, org_id: str):
        self.org_id = org_id
        self.client: Client = create_client(url, service_role_key)

    def push_sales_orders(self, orders: list[dict[str, Any]]) -> dict[str, int]:
        """
        Upsert sales orders and their line items into Supabase.

        Returns: {"headers": N, "lines": M} counts of upserted records.
        """
        header_count = 0
        line_count = 0

        for order in orders:
            lines = order.pop("lines", [])

            # Prepare header record
            header = {
                "org_id": self.org_id,
                "qb_txn_id": order["qb_txn_id"],
                "ref_number": order.get("ref_number"),
                "customer_name": order["customer_name"],
                "txn_date": order.get("txn_date"),
                "ship_date": order.get("ship_date"),
                "due_date": order.get("due_date"),
                "po_number": order.get("po_number"),
                "status": order.get("status", "open"),
                "total_amount": order.get("total_amount"),
                "memo": order.get("memo"),
                "is_fully_invoiced": order.get("is_fully_invoiced", False),
            }

            try:
                # Upsert header — conflict on (org_id, qb_txn_id)
                result = (
                    self.client.table("qb_sales_orders")
                    .upsert(header, on_conflict="org_id,qb_txn_id")
                    .execute()
                )

                if result.data:
                    so_id = result.data[0]["id"]
                    header_count += 1

                    # Upsert line items
                    for line in lines:
                        line_record = {
                            "org_id": self.org_id,
                            "sales_order_id": so_id,
                            "qb_txn_line_id": line.get("qb_txn_line_id"),
                            "line_number": line.get("line_number"),
                            "item_ref": line.get("item_ref"),
                            "description": line.get("description"),
                            "quantity": line.get("quantity"),
                            "qty_invoiced": line.get("qty_invoiced", 0),
                            "rate": line.get("rate"),
                            "amount": line.get("amount"),
                            "is_rack": line.get("is_rack", False),
                        }

                        self.client.table("qb_sales_order_lines").upsert(
                            line_record,
                            on_conflict="org_id,sales_order_id,qb_txn_line_id"
                        ).execute()
                        line_count += 1

            except Exception as e:
                logger.error(
                    f"Failed to upsert sales order {order.get('ref_number')}: {e}"
                )

        logger.info(f"Pushed {header_count} sales orders, {line_count} line items")
        return {"headers": header_count, "lines": line_count}

    def push_item_receipts(self, receipts: list[dict[str, Any]]) -> dict[str, int]:
        """
        Upsert item receipts and their line items into Supabase.

        Returns: {"headers": N, "lines": M}
        """
        header_count = 0
        line_count = 0

        for receipt in receipts:
            lines = receipt.pop("lines", [])

            header = {
                "org_id": self.org_id,
                "qb_txn_id": receipt["qb_txn_id"],
                "vendor_name": receipt.get("vendor_name"),
                "txn_date": receipt.get("txn_date"),
                "ref_number": receipt.get("ref_number"),
                "total_amount": receipt.get("total_amount"),
                "memo": receipt.get("memo"),
                "linked_po": receipt.get("linked_po"),
            }

            try:
                result = (
                    self.client.table("qb_item_receipts")
                    .upsert(header, on_conflict="org_id,qb_txn_id")
                    .execute()
                )

                if result.data:
                    ir_id = result.data[0]["id"]
                    header_count += 1

                    for line in lines:
                        line_record = {
                            "org_id": self.org_id,
                            "item_receipt_id": ir_id,
                            "item_ref": line.get("item_ref"),
                            "description": line.get("description"),
                            "quantity": line.get("quantity"),
                            "cost": line.get("cost"),
                            "amount": line.get("amount"),
                        }

                        self.client.table("qb_item_receipt_lines").upsert(
                            line_record,
                            on_conflict="org_id,item_receipt_id,item_ref"
                        ).execute()
                        line_count += 1

            except Exception as e:
                logger.error(
                    f"Failed to upsert item receipt {receipt.get('ref_number')}: {e}"
                )

        logger.info(f"Pushed {header_count} item receipts, {line_count} line items")
        return {"headers": header_count, "lines": line_count}

    def push_items(self, items: list[dict[str, Any]]) -> int:
        """
        Upsert QB items (inventory master) into Supabase.

        Returns: count of upserted records.
        """
        count = 0

        for item in items:
            record = {
                "org_id": self.org_id,
                "qb_list_id": item["qb_list_id"],
                "item_type": item.get("item_type"),
                "full_name": item["full_name"],
                "name": item.get("name"),
                "description": item.get("description"),
                "qty_on_hand": item.get("qty_on_hand"),
                "reorder_point": item.get("reorder_point"),
                "purchase_cost": item.get("purchase_cost"),
                "sales_price": item.get("sales_price"),
                "preferred_vendor": item.get("preferred_vendor"),
                "is_active": item.get("is_active", True),
                "manufacturer": item.get("manufacturer"),
            }

            try:
                self.client.table("qb_items").upsert(
                    record, on_conflict="org_id,qb_list_id"
                ).execute()
                count += 1
            except Exception as e:
                logger.error(
                    f"Failed to upsert item {item.get('full_name')}: {e}"
                )

        logger.info(f"Pushed {count} items")
        return count

    def get_last_sync_time(self) -> str | None:
        """Get the timestamp of the last successful sync for this org."""
        try:
            result = (
                self.client.table("qb_sync_log")
                .select("completed_at")
                .eq("org_id", self.org_id)
                .eq("status", "success")
                .order("completed_at", desc=True)
                .limit(1)
                .execute()
            )

            if result.data:
                return result.data[0]["completed_at"]
        except Exception as e:
            logger.warning(f"Could not fetch last sync time: {e}")

        return None

    def log_sync_start(self, sync_type: str, company_file: str) -> str:
        """Create a sync log entry and return its ID."""
        from datetime import datetime, timezone

        record = {
            "org_id": self.org_id,
            "sync_type": sync_type,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "company_file": company_file,
        }

        result = self.client.table("qb_sync_log").insert(record).execute()
        return result.data[0]["id"]

    def log_sync_complete(self, sync_id: str, records_synced: dict,
                          error_message: str | None = None) -> None:
        """Update a sync log entry with completion details."""
        from datetime import datetime, timezone

        update = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "error" if error_message else "success",
            "records_synced": records_synced,
        }

        if error_message:
            update["error_message"] = error_message

        self.client.table("qb_sync_log").update(update).eq("id", sync_id).execute()
