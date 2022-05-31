from beam_job.main import compress_files
import os


def test_compress_files(get_example_data_path, get_test_output_path):
    input_path = get_example_data_path
    gz_file = "results.jsonl.gz"
    output_gz_path = os.path.join(get_test_output_path, gz_file)
    compress_files(input_path, output_gz_path)
    assert True
