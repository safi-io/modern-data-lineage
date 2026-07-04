import dagster as dg
from jobs import mel_pg_ch_job

mel_pg_ch_schedule = dg.ScheduleDefinition(
    name="pg_ch_repliaction",
    cron_schedule="0 2 * * *", # At 02:00 AM
    target=mel_pg_ch_job,
)