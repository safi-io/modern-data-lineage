from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = "/opt/dagster/dbt/modern_data_lineage"

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
)
