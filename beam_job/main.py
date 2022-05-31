import apache_beam as beam
from beam_job.read_csv import ReadCsvFiles
from beam_job.settings import (
    OPTIONS,
    INPUT_PATTERNS,
    OUTPUT_FILE_GZ,
    OUTPUT_DIR,
    OUTPUT_FILE_SUFFIX,
    OUTPUT_FILE_PATTERN,
)
from beam_job.composite_transforms import SumCompositeTransform
from beam_job.gz_maker import compress_files


def run_pipeline():
    with beam.Pipeline(options=OPTIONS) as pipeline:
        (
            pipeline
            | "Read CSV files" >> ReadCsvFiles(INPUT_PATTERNS)
            | "sum composite transform" >> SumCompositeTransform()
            | "Write to files"
            >> beam.io.WriteToText(
                OUTPUT_FILE_PATTERN, file_name_suffix=OUTPUT_FILE_SUFFIX
            )
        )


def print_input_data():
    with beam.Pipeline(options=OPTIONS) as pipeline:
        (
            pipeline
            | "Read CSV files" >> beam.io.ReadFromText("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv")
            | "print"
            >> beam.Map(print)
        )


def main():
    print_input_data()
    run_pipeline()
    compress_files(OUTPUT_DIR, OUTPUT_FILE_GZ)
