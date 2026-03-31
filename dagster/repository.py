from dagster import job, op

@op
def hello():
    print("Dagster is working!")

@job
def test_job():
    hello()