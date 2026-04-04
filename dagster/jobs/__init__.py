from dagster import AssetSelection, define_asset_job

ingestion_job = define_asset_job(
    name="movies_job",
    selection=AssetSelection.all() - AssetSelection.groups("ingestion_assets")
)