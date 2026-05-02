from dagster import AssetSelection, define_asset_job, job
from resources import PostgresResource

from ops import register_new_files_into_postgres

bronze_logs_ingestion_job = define_asset_job(
    name="bronze_logs_ingestion_job",
    selection=AssetSelection.groups("bronze_logs_ingestion_assets"),
)


@job(resource_defs={"postgres_app": PostgresResource()})
def file_registration_job():
    register_new_files_into_postgres()


all_jobs = [bronze_logs_ingestion_job, file_registration_job]
