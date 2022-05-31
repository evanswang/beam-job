import os
from beam_job.utils import get_utc_datetime
from apache_beam.options.pipeline_options import PipelineOptions


INPUT_PATTERNS = [
    "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
]

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, "../output")
OUTPUT_FILE_NAME = "results"
OUTPUT_FILE_PATTERN = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)
OUTPUT_FILE_SUFFIX = ".jsonl"
OUTPUT_FILE_GZ = f"{OUTPUT_FILE_PATTERN}{OUTPUT_FILE_SUFFIX}.gz"

# time zone is not clear in this question, so I assume all data are in UTC.
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
LAST_SECOND_2009_STRING = "2009-12-31 23:59:59 UTC"
LAST_SECOND_2009 = get_utc_datetime(LAST_SECOND_2009_STRING)

# column names
TRANSACTION_AMOUNT_NAME = "transaction_amount"
TIMESTAMP_NAME = "timestamp"

OPTIONS = PipelineOptions(flags=[], type_check_additional="all")
