import logging

import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | job_id=%(job_id)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

base_logger = logging.getLogger(__name__)

class JobAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return msg, {"extra": {"job_id": self.extra["job_id"]}}