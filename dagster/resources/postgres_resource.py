from dagster import ConfigurableResource, EnvVar
import psycopg2


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
