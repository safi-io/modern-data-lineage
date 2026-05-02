from dagster import ConfigurableResource, EnvVar
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
