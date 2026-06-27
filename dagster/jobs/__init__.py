from dagster import AssetSelection, define_asset_job, job
from resources import PostgresResource

from ops import register_new_files_into_postgres, run_meltano_pg_ch

bronze_logs_ingestion_job = define_asset_job(
    name="bronze_logs_ingestion_job",
    selection=AssetSelection.groups("bronze_logs_ingestion_assets"),
)


@job(resource_defs={"postgres_app": PostgresResource()})
def file_registration_job():
    register_new_files_into_postgres()

@job()
def mel_pg_ch_job():
    run_meltano_pg_ch()


all_jobs = [bronze_logs_ingestion_job, file_registration_job, mel_pg_ch_job]
