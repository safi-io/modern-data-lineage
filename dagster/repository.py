from dagster import Definitions, load_assets_from_modules

from assets import ingestion, adhoc
from jobs import ingestion_job, psql_new_files
from sensors import all_sensors
from resources import all_resources

ingestion_assets = load_assets_from_modules([ingestion], group_name="ingestion_assets")
adhoc_assets = load_assets_from_modules([adhoc])


defs = Definitions(
    assets=[*ingestion_assets, *adhoc_assets],
    jobs=[ingestion_job, psql_new_files],
    sensors=all_sensors,
    resources=all_resources,
)
