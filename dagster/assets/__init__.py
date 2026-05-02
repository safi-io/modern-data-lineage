from dagster import load_assets_from_modules

import assets.adhoc_assets as adhoc_assets
import assets.bronze_logs_assets as bronze_logs_assets

adhoc_assets_defs = load_assets_from_modules([adhoc_assets])

bronze_logs_ingestion_assets_defs = load_assets_from_modules(
    [bronze_logs_assets], group_name="bronze_logs_ingestion_assets"
)

all_assets = [
    *adhoc_assets_defs,
    *bronze_logs_ingestion_assets_defs,
]
