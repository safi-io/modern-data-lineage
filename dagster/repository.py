from dagster import Definitions, load_assets_from_modules, EnvVar

from assets import ingestion, adhoc
from jobs import ingestion_job
from sensors import ingestion_sensor
from resources import MinioResource

ingestion_assets = load_assets_from_modules([ingestion], group_name="ingestion_assets")
adhoc_assets = load_assets_from_modules([adhoc])


defs = Definitions(
    assets=[*ingestion_assets, *adhoc_assets],
    jobs=[ingestion_job],
    sensors=[ingestion_sensor],
    resources={"minio": MinioResource()},
)
