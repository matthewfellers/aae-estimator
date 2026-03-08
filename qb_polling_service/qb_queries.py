"""
QBXML Request Builders — constructs XML queries for QuickBooks Desktop.

Each function returns a QBXML string ready to send via QBConnection.execute_request().
Supports incremental sync via ModifiedDateRangeFilter.
"""

from datetime import datetime, timedelta
from typing import Optional


def _wrap_qbxml(inner_xml: str, version: str = "16.0") -> str:
    """Wrap an inner request body in the standard QBXML envelope."""
    return (
        f'<?xml version="1.0" encoding="utf-8"?>\n'
        f'<?qbxml version="{version}"?>\n'
        f'<QBXML>\n'
        f'  <QBXMLMsgsRq onError="stopOnError">\n'
        f'    {inner_xml}\n'
        f'  </QBXMLMsgsRq>\n'
        f'</QBXML>'
    )


def build_sales_order_query(
    modified_since: Optional[datetime] = None,
    include_line_items: bool = True,
    version: str = "16.0"
) -> str:
    """
    Build QBXML request to query Sales Orders.

    Args:
        modified_since: Only return SOs modified after this datetime.
                       If None, returns all sales orders.
        include_line_items: Whether to include line item detail.
        version: QBXML version string.
    """
    filters = ""

    if modified_since:
        from_date = modified_since.strftime("%Y-%m-%dT%H:%M:%S")
        filters += (
            f"      <ModifiedDateRangeFilter>\n"
            f"        <FromModifiedDate>{from_date}</FromModifiedDate>\n"
            f"      </ModifiedDateRangeFilter>\n"
        )

    line_items = ""
    if include_line_items:
        line_items = "      <IncludeLineItems>true</IncludeLineItems>\n"

    inner = (
        f"<SalesOrderQueryRq>\n"
        f"{filters}"
        f"{line_items}"
        f"    </SalesOrderQueryRq>"
    )

    return _wrap_qbxml(inner, version)


def build_item_receipt_query(
    modified_since: Optional[datetime] = None,
    include_line_items: bool = True,
    version: str = "16.0"
) -> str:
    """
    Build QBXML request to query Item Receipts.

    Args:
        modified_since: Only return receipts modified after this datetime.
        include_line_items: Whether to include line item detail.
        version: QBXML version string.
    """
    filters = ""

    if modified_since:
        from_date = modified_since.strftime("%Y-%m-%dT%H:%M:%S")
        filters += (
            f"      <ModifiedDateRangeFilter>\n"
            f"        <FromModifiedDate>{from_date}</FromModifiedDate>\n"
            f"      </ModifiedDateRangeFilter>\n"
        )

    line_items = ""
    if include_line_items:
        line_items = "      <IncludeLineItems>true</IncludeLineItems>\n"

    inner = (
        f"<ItemReceiptQueryRq>\n"
        f"{filters}"
        f"{line_items}"
        f"    </ItemReceiptQueryRq>"
    )

    return _wrap_qbxml(inner, version)


def build_item_query(
    modified_since: Optional[datetime] = None,
    active_only: bool = True,
    version: str = "16.0"
) -> str:
    """
    Build QBXML request to query Inventory and Non-Inventory items.

    Uses ItemQueryRq which returns all item types (Inventory, NonInventory,
    Service, etc.). For Assembly items, use build_assembly_query() separately.

    Args:
        modified_since: Only return items modified after this datetime.
        active_only: Only return active items (default True).
        version: QBXML version string.
    """
    filters = ""

    if modified_since:
        from_date = modified_since.strftime("%Y-%m-%dT%H:%M:%S")
        filters += (
            f"      <FromModifiedDate>{from_date}</FromModifiedDate>\n"
        )

    active_filter = ""
    if active_only:
        active_filter = "      <ActiveStatus>ActiveOnly</ActiveStatus>\n"

    # Query all inventory items
    inner_inventory = (
        f"<ItemInventoryQueryRq>\n"
        f"{active_filter}"
        f"{filters}"
        f"    </ItemInventoryQueryRq>"
    )

    # Also query non-inventory items
    inner_non_inventory = (
        f"<ItemNonInventoryQueryRq>\n"
        f"{active_filter}"
        f"{filters}"
        f"    </ItemNonInventoryQueryRq>"
    )

    # Query service items
    inner_service = (
        f"<ItemServiceQueryRq>\n"
        f"{active_filter}"
        f"{filters}"
        f"    </ItemServiceQueryRq>"
    )

    # Combine all item type queries into one request
    combined = (
        f'<?xml version="1.0" encoding="utf-8"?>\n'
        f'<?qbxml version="{version}"?>\n'
        f'<QBXML>\n'
        f'  <QBXMLMsgsRq onError="continueOnError">\n'
        f'    {inner_inventory}\n'
        f'    {inner_non_inventory}\n'
        f'    {inner_service}\n'
        f'  </QBXMLMsgsRq>\n'
        f'</QBXML>'
    )

    return combined


def build_assembly_query(
    modified_since: Optional[datetime] = None,
    active_only: bool = True,
    version: str = "16.0"
) -> str:
    """
    Build QBXML request to query Assembly items (for rack → panel mappings).

    Assembly items in QB contain sub-items which can help identify
    rack → panel relationships.
    """
    filters = ""

    if modified_since:
        from_date = modified_since.strftime("%Y-%m-%dT%H:%M:%S")
        filters += (
            f"      <FromModifiedDate>{from_date}</FromModifiedDate>\n"
        )

    active_filter = ""
    if active_only:
        active_filter = "      <ActiveStatus>ActiveOnly</ActiveStatus>\n"

    inner = (
        f"<ItemInventoryAssemblyQueryRq>\n"
        f"{active_filter}"
        f"{filters}"
        f"    </ItemInventoryAssemblyQueryRq>"
    )

    return _wrap_qbxml(inner, version)


def build_company_query(version: str = "16.0") -> str:
    """Build a simple company info query — useful for connection testing."""
    return _wrap_qbxml("<CompanyQueryRq></CompanyQueryRq>", version)
