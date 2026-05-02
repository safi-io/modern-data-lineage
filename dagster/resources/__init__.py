from .minio_resource import MinioResource
from .postgres_resource import PostgresResource

all_resources = {
    "minio": MinioResource(),
    "postgres_app": PostgresResource(),
}
