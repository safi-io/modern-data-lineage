from dagster import AutoMaterializePolicy
from dagster_dbt import DagsterDbtTranslator

from typing import Any, Mapping, Optional

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        folder_name = f"dbt_assets_{dbt_resource_props['schema']}"
        return folder_name

    def get_auto_materialize_policy(
        self,
        dbt_resource_props: Mapping[str, Any],
    ) -> Optional[AutoMaterializePolicy]:

        return AutoMaterializePolicy.eager()