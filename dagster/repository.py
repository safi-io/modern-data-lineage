from dagster import Definitions

from assets import all_assets
from jobs import all_jobs
from sensors import all_sensors
from resources import all_resources
from schedules import all_schedules

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    sensors=all_sensors,
    resources=all_resources,
    schedules=all_schedules
)
