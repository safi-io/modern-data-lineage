from dagster import job, op
import pandas as pd
import dlt

@op
def load_csv_data():
    df = pd.read_csv("./data/logs.csv")

    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="filesystem",
        dataset_name="mydata",
    )

    load_info = pipeline.run(df, table_name="logs_data")

    print(load_info)

@job
def dlt_pipeline_job():
    load_csv_data()