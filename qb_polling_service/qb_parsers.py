"""
QB XML Response Parsers — converts QBXML responses into Python dicts.

Each parser handles a specific QB entity type and returns a list of
normalized dictionaries ready for Supabase upsert.
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

logger = logging.getLogger("qb_poller")

# Known part number prefixes → manufacturer mappings
MANUFACTURER_PREFIXES = {
    "1756": "Allen-Bradley",
    "1769": "Allen-Bradley",
    "1771": "Allen-Bradley",
    "1746": "Allen-Bradley",
    "1734": "Allen-Bradley",
    "2711": "Allen-Bradley",
    "100-": "Allen-Bradley",
    "140M": "Allen-Bradley",
    "150-": "Allen-Bradley",
    "160-": "Allen-Bradley",
    "190-": "Allen-Bradley",
    "193-": "Allen-Bradley",
    "194-": "Allen-Bradley",
    "195-": "Allen-Bradley",
    "500-": "Allen-Bradley",
    "509-": "Allen-Bradley",
    "520-": "Allen-Bradley",
    "700-": "Allen-Bradley",
    "800F": "Allen-Bradley",
    "800T": "Allen-Bradley",
    "ATV": "Schneider Electric",
    "GV2": "Schneider Electric",
    "LC1": "Schneider Electric",
    "LR": "Schneider Electric",
    "EHD": "Hoffman",
    "A-": "Hoffman",
    "SCE": "Saginaw",
    "E1P": "Panduit",
    "S100": "Panduit",
    "KLSR": "Littelfuse",
    "LP-C": "Littelfuse",
    "IDEC": "IDEC",
    "RH": "IDEC",
    "SY4": "IDEC",
    "ABB": "ABB",
    "1SVR": "ABB",
    "A9": "ABB",
    "WDU": "Weidmuller",
    "WPE": "Weidmuller",
    "ZDU": "Weidmuller",
    "WAGO": "WAGO",
}


def _get_text(element: ET.Element, tag: str, default: str = "") -> str:
    """Safely extract text from an XML child element."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def _get_float(element: ET.Element, tag: str, default: float = 0.0) -> float:
    """Safely extract a float from an XML child element."""
    text = _get_text(element, tag)
    if text:
        try:
            return float(text)
        except ValueError:
            return default
    return default


def _get_date(element: ET.Element, tag: str) -> str | None:
    """Extract a date string (YYYY-MM-DD) from an XML element."""
    text = _get_text(element, tag)
    if text:
        # QB dates can be YYYY-MM-DD or YYYY-MM-DDThh:mm:ss
        return text[:10] if len(text) >= 10 else text
    return None


def _get_ref_value(element: ET.Element, ref_tag: str) -> str:
    """Extract the FullName from a QB Ref element (e.g., CustomerRef/FullName)."""
    ref = element.find(ref_tag)
    if ref is not None:
        full_name = ref.find("FullName")
        if full_name is not None and full_name.text:
            return full_name.text.strip()
    return ""


def _get_ref_list_id(element: ET.Element, ref_tag: str) -> str:
    """Extract the ListID from a QB Ref element."""
    ref = element.find(ref_tag)
    if ref is not None:
        list_id = ref.find("ListID")
        if list_id is not None and list_id.text:
            return list_id.text.strip()
    return ""


def _guess_manufacturer(part_number: str) -> str:
    """Attempt to identify manufacturer from part number prefix."""
    if not part_number:
        return ""
    upper = part_number.upper()
    for prefix, mfr in MANUFACTURER_PREFIXES.items():
        if upper.startswith(prefix.upper()):
            return mfr
    return ""


def parse_sales_orders(xml_response: str) -> list[dict[str, Any]]:
    """
    Parse SalesOrderQueryRs XML into a list of sales order dicts.

    Each dict contains the header fields plus a 'lines' key with line items.
    Returns: [
        {
            'qb_txn_id': '...',
            'ref_number': 'SO-1234',
            'customer_name': 'Devon Energy',
            'txn_date': '2026-01-15',
            'ship_date': '2026-03-01',
            'due_date': '2026-04-01',
            'po_number': 'PO-5678',
            'status': 'open',
            'total_amount': 125000.00,
            'memo': '...',
            'is_fully_invoiced': False,
            'lines': [
                {
                    'qb_txn_line_id': '...',
                    'line_number': 1,
                    'item_ref': 'PNL-VFD-001',
                    'description': 'VFD Panel Assembly',
                    'quantity': 2.0,
                    'qty_invoiced': 0.0,
                    'rate': 15000.00,
                    'amount': 30000.00,
                    'is_rack': False
                },
                ...
            ]
        },
        ...
    ]
    """
    orders = []

    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as e:
        logger.error(f"Failed to parse sales order XML: {e}")
        return orders

    # Find all SalesOrderRet elements
    for so_ret in root.iter("SalesOrderRet"):
        txn_id = _get_text(so_ret, "TxnID")
        if not txn_id:
            continue

        # Determine status from IsManuallyClosed and IsFullyInvoiced
        is_closed = _get_text(so_ret, "IsManuallyClosed", "false").lower() == "true"
        is_invoiced = _get_text(so_ret, "IsFullyInvoiced", "false").lower() == "true"
        status = "closed" if (is_closed or is_invoiced) else "open"

        order = {
            "qb_txn_id": txn_id,
            "ref_number": _get_text(so_ret, "RefNumber"),
            "customer_name": _get_ref_value(so_ret, "CustomerRef"),
            "txn_date": _get_date(so_ret, "TxnDate"),
            "ship_date": _get_date(so_ret, "ShipDate"),
            "due_date": _get_date(so_ret, "DueDate"),
            "po_number": _get_text(so_ret, "PONumber"),
            "status": status,
            "total_amount": _get_float(so_ret, "TotalAmount"),
            "memo": _get_text(so_ret, "Memo"),
            "is_fully_invoiced": is_invoiced,
            "lines": [],
        }

        # Parse line items
        line_num = 0
        for line_tag in ["SalesOrderLineRet", "SalesOrderLineGroupRet"]:
            for line in so_ret.iter(line_tag):
                line_num += 1
                item_ref = _get_ref_value(line, "ItemRef")

                # Detect rack items by naming convention
                is_rack = False
                if item_ref:
                    upper_ref = item_ref.upper()
                    is_rack = any(
                        kw in upper_ref
                        for kw in ["RACK", "RCK-", "ASSEMBLY", "ASSY"]
                    )

                line_data = {
                    "qb_txn_line_id": _get_text(line, "TxnLineID"),
                    "line_number": line_num,
                    "item_ref": item_ref,
                    "description": _get_text(line, "Desc"),
                    "quantity": _get_float(line, "Quantity"),
                    "qty_invoiced": _get_float(line, "Invoiced"),
                    "rate": _get_float(line, "Rate"),
                    "amount": _get_float(line, "Amount"),
                    "is_rack": is_rack,
                }
                order["lines"].append(line_data)

        orders.append(order)

    logger.info(f"Parsed {len(orders)} sales orders")
    return orders


def parse_item_receipts(xml_response: str) -> list[dict[str, Any]]:
    """
    Parse ItemReceiptQueryRs XML into a list of receipt dicts.

    Each dict contains header fields plus a 'lines' key with received items.
    """
    receipts = []

    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as e:
        logger.error(f"Failed to parse item receipt XML: {e}")
        return receipts

    for ir_ret in root.iter("ItemReceiptRet"):
        txn_id = _get_text(ir_ret, "TxnID")
        if not txn_id:
            continue

        receipt = {
            "qb_txn_id": txn_id,
            "vendor_name": _get_ref_value(ir_ret, "VendorRef"),
            "txn_date": _get_date(ir_ret, "TxnDate"),
            "ref_number": _get_text(ir_ret, "RefNumber"),
            "total_amount": _get_float(ir_ret, "TotalAmount"),
            "memo": _get_text(ir_ret, "Memo"),
            "linked_po": "",
            "lines": [],
        }

        # Try to extract linked PO from LinkedTxn
        for linked in ir_ret.iter("LinkedTxn"):
            txn_type = _get_text(linked, "TxnType")
            if txn_type == "PurchaseOrder":
                receipt["linked_po"] = _get_text(linked, "RefNumber")
                break

        # Parse line items
        for line_tag in ["ItemLineRet", "ItemGroupLineRet"]:
            for line in ir_ret.iter(line_tag):
                line_data = {
                    "item_ref": _get_ref_value(line, "ItemRef"),
                    "description": _get_text(line, "Desc"),
                    "quantity": _get_float(line, "Quantity"),
                    "cost": _get_float(line, "Cost"),
                    "amount": _get_float(line, "Amount"),
                }
                if line_data["item_ref"]:
                    receipt["lines"].append(line_data)

        receipts.append(receipt)

    logger.info(f"Parsed {len(receipts)} item receipts")
    return receipts


def parse_items(xml_response: str, item_type: str = "Inventory") -> list[dict[str, Any]]:
    """
    Parse ItemInventoryQueryRs / ItemNonInventoryQueryRs / ItemServiceQueryRs
    into a list of item dicts.

    Args:
        xml_response: The QBXML response string.
        item_type: The type tag to look for (Inventory, NonInventory, Service).
    """
    items = []

    # Map item types to their XML return tags
    type_to_tag = {
        "Inventory": "ItemInventoryRet",
        "NonInventory": "ItemNonInventoryRet",
        "Service": "ItemServiceRet",
        "Assembly": "ItemInventoryAssemblyRet",
    }

    ret_tag = type_to_tag.get(item_type, f"Item{item_type}Ret")

    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as e:
        logger.error(f"Failed to parse item XML: {e}")
        return items

    for item_ret in root.iter(ret_tag):
        list_id = _get_text(item_ret, "ListID")
        if not list_id:
            continue

        full_name = _get_text(item_ret, "FullName")
        name = _get_text(item_ret, "Name")

        item = {
            "qb_list_id": list_id,
            "item_type": item_type,
            "full_name": full_name,
            "name": name,
            "description": (
                _get_text(item_ret, "SalesDesc")
                or _get_text(item_ret, "PurchaseDesc")
                or _get_text(item_ret, "Name")
            ),
            "qty_on_hand": _get_float(item_ret, "QuantityOnHand"),
            "reorder_point": _get_float(item_ret, "ReorderPoint"),
            "purchase_cost": _get_float(item_ret, "PurchaseCost"),
            "sales_price": _get_float(item_ret, "SalesPrice"),
            "preferred_vendor": _get_ref_value(item_ret, "PrefVendorRef"),
            "is_active": _get_text(item_ret, "IsActive", "true").lower() == "true",
            "manufacturer": _guess_manufacturer(name or full_name),
        }

        items.append(item)

    logger.info(f"Parsed {len(items)} {item_type} items")
    return items


def parse_all_items(xml_response: str) -> list[dict[str, Any]]:
    """
    Parse a combined item query response that may contain multiple item types.

    The build_item_query() function sends Inventory, NonInventory, and Service
    queries in one request. This function parses all three response types.
    """
    all_items = []

    for item_type in ["Inventory", "NonInventory", "Service"]:
        items = parse_items(xml_response, item_type)
        all_items.extend(items)

    logger.info(f"Parsed {len(all_items)} total items across all types")
    return all_items


def check_response_status(xml_response: str) -> tuple[bool, str]:
    """
    Check the statusCode of a QBXML response.

    Returns: (success: bool, message: str)
    Status codes:
        0    = Success
        1    = No data found (not an error)
        500+ = Error
    """
    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as e:
        return False, f"XML parse error: {e}"

    for element in root.iter():
        status_code = element.get("statusCode")
        if status_code is not None:
            code = int(status_code)
            message = element.get("statusMessage", "")
            if code == 0:
                return True, "Success"
            elif code == 1:
                return True, f"No data found: {message}"
            else:
                return False, f"Error {code}: {message}"

    return True, "No status found in response"
