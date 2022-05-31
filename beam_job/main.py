import apache_beam as beam
from beam_job.read_csv import ReadCsvFiles
import json
import gzip
import shutil
from beam_job.settings import (
    OPTIONS,
    INPUT_PATTERNS,
    TRANSACTION_AMOUNT_NAME,
    LAST_SECOND_2009_STRING,
    LAST_SECOND_2009,
    TIMESTAMP_NAME,
    OUTPUT_FILE_PATTERN,
    OUTPUT_FILE_SUFFIX,
    OUTPUT_FILE_GZ,
    OUTPUT_DIR
)
from beam_job.utils import get_utc_datetime
from os import walk
import os


def run_pipeline():
    with beam.Pipeline(options=OPTIONS) as pipeline:
        (
            pipeline
            | "Read CSV files" >> ReadCsvFiles(INPUT_PATTERNS)
            | "filter transaction_amount > 20.0"
            >> beam.Filter(lambda x: float(x[TRANSACTION_AMOUNT_NAME]) > 20)
            | f"filter timestamp > {LAST_SECOND_2009_STRING}"
            >> beam.Filter(
                lambda x: get_utc_datetime(x[TIMESTAMP_NAME]) > LAST_SECOND_2009
            )
            | "dict to tuple"
            >> beam.Map(
                lambda x: (
                    f"{get_utc_datetime(x[TIMESTAMP_NAME]).date()}",
                    float(x[TRANSACTION_AMOUNT_NAME]),
                )
            )
            | "Combine per date" >> beam.CombinePerKey(sum)
            | "To print format"
            >> beam.Map(lambda x: json.dumps({"date": x[0], "total_amount": x[1]}))
            | "Write to files"
            >> beam.io.WriteToText(
                OUTPUT_FILE_PATTERN, file_name_suffix=OUTPUT_FILE_SUFFIX
            )
            # | "Print elements" >> beam.Map(print)
        )


def compress_files(input_folder: str, output_file: str):
    with gzip.open(output_file, "wb") as f_out:
        for (dirpath, dirnames, filenames) in walk(input_folder):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                with open(file_path, "rb") as f_in:
                    shutil.copyfileobj(f_in, f_out)


if __name__ == "__main__":
    run_pipeline()
    compress_files(OUTPUT_DIR, OUTPUT_FILE_GZ)

