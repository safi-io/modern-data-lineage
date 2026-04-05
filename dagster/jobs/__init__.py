from dagster import AssetSelection, define_asset_job

ingestion_job = define_asset_job(
    name="ingestion_job", selection=AssetSelection.groups("ingestion_assets")
)
