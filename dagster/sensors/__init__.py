from .s3_csv_check_sensor import s3_csv_check_sensor
from .postgres_pending_files_ingestion_sensor import (
    postgres_pending_files_ingestion_sensor,
)

all_sensors = [s3_csv_check_sensor, postgres_pending_files_ingestion_sensor]
