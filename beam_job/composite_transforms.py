import apache_beam as beam
from beam_job.settings import (
    TRANSACTION_AMOUNT_COLUMN,
    LAST_SECOND_2009_STRING,
    LAST_SECOND_2009,
    TIMESTAMP_COLUMN,
    DATE_COLUMN,
    TOTAL_AMOUNT_COLUMN,
)
from beam_job.utils import get_utc_datetime


class SumCompositeTransform(beam.PTransform):
    def expand(self, input_col):
        pipeline = (
            input_col
            | "filter transaction_amount > 20.0"
            >> beam.Filter(lambda row: float(row[TRANSACTION_AMOUNT_COLUMN]) > 20)
            | f"filter timestamp > {LAST_SECOND_2009_STRING}"
            >> beam.Filter(
                lambda row: get_utc_datetime(row[TIMESTAMP_COLUMN]) > LAST_SECOND_2009
            )
            | "dict to tuple"
            >> beam.Map(
                lambda row: (
                    f"{get_utc_datetime(row[TIMESTAMP_COLUMN]).date()}",
                    float(row[TRANSACTION_AMOUNT_COLUMN]),
                )
            )
            | "Combine per date" >> beam.CombinePerKey(sum)
            | "To dict format"
            >> beam.Map(lambda x: {DATE_COLUMN: x[0], TOTAL_AMOUNT_COLUMN: x[1]})
        )
        return pipeline
