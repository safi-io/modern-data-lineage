from dagster import asset
import dlt
import pandas as pd


@asset
def csv_to_minio():

    LOCAL_CSV_PATH = ""

    df = pd.read_csv(LOCAL_CSV_PATH)

    # At destination, the CSV will be automatically converted to Parquet Format.

    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="filesystem",
        dataset_name="logs",
    )
    pipeline.run(df, table_name="logs_data")
    return "Logs in parquet format loaded into minio"
