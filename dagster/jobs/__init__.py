from dagster import AssetSelection, define_asset_job, job, op
from resources import PostgresResource

ingestion_job = define_asset_job(
    name="ingestion_job", selection=AssetSelection.groups("ingestion_assets")
)

# TODO: Refactor the op.
@op(config_schema={"files": list}, required_resource_keys={"postgres_app"})
def process_files(context):
    files = context.op_config["files"]

    conn = context.resources.postgres_app.get_connection()
    cursor = conn.cursor()

    try:
        for f in files:
            cursor.execute(
                """
                INSERT INTO file_status (file_name, status)
                VALUES (%s, 'new')
                ON CONFLICT (file_name) DO NOTHING
            """,
                (f,),
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()


@job(resource_defs={"postgres_app": PostgresResource()})
def psql_new_files():
    process_files()
