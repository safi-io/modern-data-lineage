from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from .utils.dagster_utils import CustomDagsterDbtTranslator

DBT_PROJECT_DIR = "/opt/dagster/dbt/modern_data_lineage"

# Cleaned Assets
@dbt_assets(
    manifest=Path(DBT_PROJECT_DIR, "target", "manifest.json"),
    select="tag:cleaned",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def cleaned_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--select", "tag:cleaned"], context=context).stream()