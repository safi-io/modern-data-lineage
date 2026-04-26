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
    host: str = EnvVar("POSTGRES_APP_HOST")
    port: int = EnvVar.int("POSTGRES_APP_PORT")
    user: str = EnvVar("POSTGRES_APP_USER")
    password: str = EnvVar("POSTGRES_APP_PASSWORD")
    database: str = EnvVar("POSTGRES_APP_DATABASE")

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )