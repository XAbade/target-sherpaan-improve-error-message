"""Target Sherpaan class."""

from __future__ import annotations

from typing import List

from singer_sdk import typing as th

from target_sherpaan import sinks
from target_hotglue.target import TargetHotglue


class TargetSherpaan(TargetHotglue):
    """Target for Sherpaan PurchaseOrders."""

    name = "target-sherpaan"
    # Register available sinks for TargetHotglue SDK (mirrors pattern in other targets)
    SINK_TYPES = [sinks.PurchaseOrderSink]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "shop_id",
            th.StringType,
            required=True,
            description="The shop ID for the Sherpa SOAP service",
        ),
        th.Property(
            "security_code",
            th.StringType,
            required=True,
            description="Security code for authentication",
        ),
        th.Property(
            "export_buyOrder_warehouse",
            th.StringType,
            required=True,
            description="Default warehouse code to use for purchase orders when not provided in record",
        ),
        th.Property(
            "base_url",
            th.StringType,
            description="Base URL for the Sherpa service (defaults to test environment)",
            default="https://sherpaservices-prd.sherpacloud.eu",
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Request timeout in seconds",
            default=300,
        ),
        th.Property(
            "export_buyOrderId_to",
            th.StringType,
            required=False,
            description="Defines the attribute to where we will send OptiplyID. Usually: reference or externalOrderNumber",
        )
    ).to_dict()

    def get_sinks(self) -> List[sinks.PurchaseOrderSink]:
        """Return a list of sinks."""
        return [
            sinks.PurchaseOrderSink(
                target=self,
                stream_name="purchase_orders",
            )
        ]


if __name__ == "__main__":
    TargetSherpaan.cli()
