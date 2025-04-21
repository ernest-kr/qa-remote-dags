from __future__ import annotations

from airflow.sdk.definitions.asset.decorators import asset

docs = """
Two assets can't use same uri. Only 1 asset (the last defined) is active if same uri is given to the multiple assets.
In the below scenario, 'same_uri_producer1' should fail and 'same_uri_producer2' should pass.
"""


@asset(uri="s3://bucket/same_uri_producer", schedule=None, tags=["AIP-75"])
def same_uri_producer1():
    pass


@asset(uri="s3://bucket/same_uri_producer", schedule=None, tags=["AIP-75"])
def same_uri_producer2():
    pass
