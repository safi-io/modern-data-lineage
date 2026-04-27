from dagster import ConfigurableResource, EnvVar
import psycopg2
from minio import Minio


class MinioResource(ConfigurableResource):
    endpoint: str = EnvVar("MINIO_ENDPOINT_RESOURCE")
    access_key: str = EnvVar("MINIO_ACCESS_KEY")
    secret_key: str = EnvVar("MINIO_SECRET_KEY")

    def get_client(self):
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

class PostgresResource(ConfigurableResource):
    host: str = EnvVar("DAGSTER_PG_HOST")
    port: int = EnvVar.int("DAGSTER_PG_PORT")
    user: str = EnvVar("DAGSTER_PG_USER")
    password: str = EnvVar("DAGSTER_PG_PASSWORD")
    database: str = EnvVar("DAGSTER_CUSTOM_PG_DB")

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )