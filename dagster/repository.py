from dagster import asset, Definitions, PipesSubprocessClient
import pandas as pd
import dlt

@asset
def logs_data():
    df = pd.read_csv("./data/logs.csv")
    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="filesystem",
        dataset_name="logs",
    )
    pipeline.run(df, table_name="logs_data")
    return "loaded" 

@asset(deps=[logs_data])
def spark_processed_data(context, pipes_client: PipesSubprocessClient):
    command = [
        "docker", "exec", 
        "-e", "DAGSTER_PIPES_CONTEXT",
        "-e", "DAGSTER_PIPES_MESSAGES", 
        "spark", 
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark:7077",
        "/opt/spark-apps/spark_iceberg_nessie.py"
    ]
    
    return pipes_client.run(
        command=command,
        context=context,
    ).get_results()

defs = Definitions(
    assets=[logs_data, spark_processed_data],
    resources={
        "pipes_client": PipesSubprocessClient(),
    }
)