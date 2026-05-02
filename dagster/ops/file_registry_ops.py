from dagster import op


@op(config_schema={"files": list}, required_resource_keys={"postgres_app"})
def register_new_files_into_postgres(context):
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
