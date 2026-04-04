from dagster import Definitions, load_assets_from_modules

from assets import ingestion, adhoc
from jobs import ingestion_job

ingestion_assets = load_assets_from_modules([ingestion], group_name="ingestion")
adhoc_assets = load_assets_from_modules([adhoc])


defs = Definitions(
    assets=[*ingestion_assets, *adhoc_assets],
    jobs=[ingestion_job]
)