import sys
import logging

def get_spark_logger(job_id):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | job_id=%(job_id)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    base_logger = logging.getLogger(__name__)

    class JobAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return msg, {"extra": {"job_id": self.extra["job_id"]}}

    return JobAdapter(base_logger, {"job_id": job_id})