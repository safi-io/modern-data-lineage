from .minio_resource import MinioResource
from .postgres_resource import PostgresResource
from .dbt_resource import dbt_resource

all_resources = {
    "minio": MinioResource(),
    "postgres_app": PostgresResource(),
    "dbt": dbt_resource
}
