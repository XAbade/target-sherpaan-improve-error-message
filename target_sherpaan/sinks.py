"""Sink classes for target-sherpaan."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from xml.sax.saxutils import escape

from singer_sdk import typing as th
from target_hotglue.client import HotglueSink

from target_sherpaan.client import SherpaClient
from target_sherpaan.auth import SherpaAuth


class PurchaseOrderSink(HotglueSink):
    """Sink for PurchaseOrders."""

    # Stream name must match the incoming Singer stream, which is "BuyOrders"
    # in the data.singer payload delivered by Hotglue.
    name = "BuyOrders"
    schema = th.PropertiesList(
        th.Property("supplier_remoteId", th.StringType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("warehouse_code", th.StringType, required=False),
        th.Property("created_at", th.StringType),
        th.Property("transaction_date", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("line_items", th.ArrayType(
            th.ObjectType(
                th.Property("product_remoteId", th.StringType, required=True),
                th.Property("supplier_item_code", th.StringType),
                th.Property("quantity", th.NumberType, required=True),
                th.Property("unit_price", th.NumberType, required=True),
            )
        ), required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        """Initialize the sink."""
        # When used with TargetHotglue, the SDK will pass `schema` and
        # `key_properties` into the Sink constructor. Passing them again here
        # would cause `TypeError: ... got multiple values for keyword 'schema'`,
        # so we simply forward all args/kwargs to the base class unchanged.
        super().__init__(*args, **kwargs)
        auth = SherpaAuth(self.config)
        timeout = self.config.get("timeout", 300)
        self.client = SherpaClient(auth, timeout=timeout)
        self.logger = logging.getLogger(__name__)

    def preprocess_record(self, record: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Preprocess record before upserting.
        
        Args:
            record: Record to preprocess
            context: Optional context dictionary
            
        Returns:
            Preprocessed record
        """
        return record

    def _format_expected_date(self, created_at: Optional[str]) -> str:
        """Format date to YYYY-MM-DDTHH:MM:SS.000 format (no timezone).
        
        Args:
            created_at: ISO date string or None
            
        Returns:
            Formatted date string
        """
        if created_at:
            try:
                if isinstance(created_at, str):
                    # Handle ISO format with microseconds and timezone
                    pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2}|Z)?'
                    match = re.match(pattern, created_at)
                    if match:
                        base_time = match.group(1)
                        microseconds = match.group(2)
                        timezone = match.group(3) or ""
                        fractional = microseconds[:3].ljust(3, "0")
                        date_str = f"{base_time}.{fractional}{timezone}".replace("Z", "+00:00")
                    else:
                        date_str = created_at.replace("Z", "+00:00")
                    
                    dt = datetime.fromisoformat(date_str)
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                elif isinstance(created_at, datetime):
                    dt = created_at
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                else:
                    return str(created_at)
            except Exception as e:
                self.logger.warning(f"Failed to parse created_at date '{created_at}': {e}, using default")
        
        # Default to 30 days from now
        return (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000")

    def _build_add_ordered_purchase_envelope(
        self,
        supplier_code: str,
        reference: str,
        warehouse_code: str
    ) -> str:
        """Build SOAP envelope for AddOrderedPurchaseWithExternalOrderNumber.

        Args:
            supplier_code: Supplier code
            reference: Reference for the purchase order
            warehouse_code: Warehouse code

        Returns:
            SOAP envelope XML string
        """
        security_code = self.config["security_code"]
        export_to = self.config.get("export_buyOrderId_to")

        if export_to == "reference":
            id_xml = f"      <reference>{escape(reference)}</reference>"
        elif export_to == "externalOrderNumber":
            id_xml = f"      <externalOrderNumber>{escape(reference)}</externalOrderNumber>"
        else:
            id_xml = f"      <reference>{escape(reference)}</reference>\n      <externalOrderNumber>{escape(reference)}</externalOrderNumber>"

        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <AddOrderedPurchaseWithExternalOrderNumber xmlns="http://sherpa.sherpaan.nl/">
      <securityCode>{escape(str(security_code))}</securityCode>
      <supplierCode>{escape(str(supplier_code))}</supplierCode>
{id_xml}
      <warehouseCode>{escape(str(warehouse_code))}</warehouseCode>
    </AddOrderedPurchaseWithExternalOrderNumber>
  </soap12:Body>
</soap12:Envelope>"""

    def _build_change_purchase2_envelope(
        self,
        purchase_order_number: str,
        line_items: list[Dict[str, Any]],
        created_at: Optional[str] = None
    ) -> str:
        """Build SOAP envelope for ChangePurchase2.

        Args:
            purchase_order_number: Purchase order number from AddOrderedPurchaseWithExternalOrderNumber
            line_items: List of line item dictionaries
            created_at: Expected date from order level (used for all lines)

        Returns:
            SOAP envelope XML string
        """
        security_code = self.config["security_code"]
        
        # Format expected date: YYYY-MM-DDTHH:MM:SS.000 (no timezone, 3 digits for milliseconds)
        formatted_date = self._format_expected_date(created_at)
        
        purchase_lines_xml = ""
        for line in line_items:
            unit_price_for_soap = line.get("unit_price")
            
            # Skip lines without unit_price (required field)
            if unit_price_for_soap is None:
                item_code = line.get("product_remoteId", "")
                self.logger.warning(f"Skipping line item {item_code}: missing required unit_price")
                continue
            
            item_code_for_soap = line.get("product_remoteId", "")
            supplier_item_code_for_soap = line.get("supplier_item_code", item_code_for_soap)
            quantity_ordered_for_soap = line.get("quantity", 0)
            
            purchase_lines_xml += f"""      <ChangePurchaseLine>
        <ItemCode>{escape(str(item_code_for_soap))}</ItemCode>
        <SupplierItemCode>{escape(str(supplier_item_code_for_soap))}</SupplierItemCode>
        <QuantityOrdered>{escape(str(quantity_ordered_for_soap))}</QuantityOrdered>
        <Price>{escape(str(unit_price_for_soap))}</Price>
        <ExpectedDate>{formatted_date}</ExpectedDate>
      </ChangePurchaseLine>
"""

        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <ChangePurchase2 xmlns="http://sherpa.sherpaan.nl/">
      <securityCode>{escape(str(security_code))}</securityCode>
      <purchaseOrderNumber>{escape(str(purchase_order_number))}</purchaseOrderNumber>
      <purchaseLines>
{purchase_lines_xml}      </purchaseLines>
    </ChangePurchase2>
  </soap12:Body>
</soap12:Envelope>"""

    def _extract_purchase_order_number(self, response: Dict[str, Any]) -> Optional[str]:
        """Extract purchase order number from AddOrderedPurchaseWithExternalOrderNumber response.

        Args:
            response: Parsed SOAP response

        Returns:
            Purchase order number or None
        """
        # Try different possible response structures
        if isinstance(response, dict):
            # First, look for ResponseValue which contains the actual purchase order number
            # Response structure: {'AddOrderedPurchaseWithExternalOrderNumberResult': {'ResponseValue': '600010', 'ResponseTime': '61'}}
            for key, value in response.items():
                if isinstance(value, dict):
                    # Check for ResponseValue in nested dicts
                    if "ResponseValue" in value:
                        response_value = value["ResponseValue"]
                        if response_value:
                            return str(response_value)
                    # Recursively check nested structures
                    result = self._extract_purchase_order_number(value)
                    if result:
                        return result
            
            # Look for common field names directly
            for key in ["PurchaseOrderNumber", "purchaseOrderNumber", "PurchaseNumber", "purchaseNumber", "OrderNumber", "orderNumber", "ResponseValue"]:
                if key in response:
                    value = response[key]
                    if value and str(value).isdigit():
                        return str(value)
            
            # Last resort: look for any numeric string value (but skip ResponseTime)
            for key, value in response.items():
                if key != "ResponseTime" and isinstance(value, str) and value.isdigit():
                    return value

        return None

    def upsert_record(self, record: Dict[str, Any], context: Optional[Dict[str, Any]] = None):
        """Process a single record and return metrics for hotglue SDK.

        Args:
            record: Record to process
            context: Optional context dictionary

        Returns:
            Tuple of (id, status, state_updates) for metrics tracking
        """
        status = True
        state_updates = dict()
        purchase_order_number = None

        try:
            supplier_code_for_soap = record["supplier_remoteId"]
            reference_for_soap = str(record["id"])
            warehouse_code_for_soap = record.get("warehouse_code") or self.config.get("export_buyOrder_warehouse")
            if not warehouse_code_for_soap:
                raise ValueError("warehouse_code is required but not found in record or config (export_buyOrder_warehouse)")
            
            # Parse line_items if it's a JSON string
            line_items_raw = record.get("line_items", [])
            if isinstance(line_items_raw, str):
                try:
                    line_items = json.loads(line_items_raw)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse line_items JSON string: {e}")
                    raise ValueError(f"Invalid JSON in line_items: {e}")
            else:
                line_items = line_items_raw if isinstance(line_items_raw, list) else []
            
            created_at = record.get("created_at")

            if not line_items:
                self.logger.warning(f"No line items found for order id {reference_for_soap}, skipping")
                state_updates["success"] = False
                status = False
                return None, status, state_updates

            self.logger.info(f"Creating purchase order with id: {reference_for_soap}")

            add_envelope = self._build_add_ordered_purchase_envelope(
                supplier_code=supplier_code_for_soap,
                reference=reference_for_soap,
                warehouse_code=warehouse_code_for_soap
            )

            add_response = self.client.call_soap_service(
                service_name="AddOrderedPurchaseWithExternalOrderNumber",
                soap_envelope=add_envelope
            )

            self.logger.debug(f"AddOrderedPurchaseWithExternalOrderNumber response: {add_response}")

            # Extract purchase order number from response
            purchase_order_number = self._extract_purchase_order_number(add_response)

            if not purchase_order_number:
                self.logger.error(
                    f"Failed to extract purchase order number from AddOrderedPurchaseWithExternalOrderNumber response"
                )
                self.logger.error(f"Full response structure: {add_response}")
                self.logger.error(f"Response type: {type(add_response)}")
                state_updates["success"] = False
                state_updates["error"] = f"Could not extract purchase order number from response: {add_response}"
                status = False
                return None, status, state_updates

            self.logger.info(f"Created purchase order {purchase_order_number} for order id {reference_for_soap}")
            self.logger.info(f"Adding {len(line_items)} line items to order {purchase_order_number}")

            change_envelope = self._build_change_purchase2_envelope(
                purchase_order_number=purchase_order_number,
                line_items=line_items,
                created_at=created_at
            )

            change_response = self.client.call_soap_service(
                service_name="ChangePurchase2",
                soap_envelope=change_envelope
            )

            self.logger.info(
                f"Successfully processed purchase order {purchase_order_number} "
                f"with {len(line_items)} lines for order id {reference_for_soap}"
            )

            state_updates["success"] = True
            return purchase_order_number, status, state_updates

        except Exception as e:
            import traceback
            from tenacity import RetryError
            actual_error = e.last_attempt.exception() if isinstance(e, RetryError) else e
            self.logger.error(f"Error processing record {record.get('id', 'unknown')}: {actual_error}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            state_updates["success"] = False
            state_updates["error"] = str(actual_error)
            status = False
            return purchase_order_number, status, state_updates
