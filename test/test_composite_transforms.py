from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from beam_job.composite_transforms import SumCompositeTransform
import apache_beam as beam
from test.example_data.input_sample import TEST_SAMPLE


def test_sum_composite_transform():
    # Create a test pipeline.
    with TestPipeline() as p:
        # Create an input PCollection.
        input = p | beam.Create(TEST_SAMPLE)
        # Apply the Count transform under test.
        output = input | SumCompositeTransform()

        # Assert on the results.
        assert_that(
            output,
            equal_to(
                [
                    {"date": "2017-03-18", "total_amount": 2102.22},
                    {"date": "2017-08-31", "total_amount": 13700000023.08},
                    {"date": "2018-02-27", "total_amount": 129.12},
                ]
            ),
        )
