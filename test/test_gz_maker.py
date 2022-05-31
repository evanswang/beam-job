from beam_job.gz_maker import compress_files
import os


def test_compress_files(get_example_data_path, get_test_output_path):
    input_path = get_example_data_path
    gz_file = "results.jsonl.gz"
    output_gz_path = os.path.join(get_test_output_path, gz_file)
    compress_files(input_path, output_gz_path)
    with open(output_gz_path, "rb") as test_f:
        assert test_f.read(2) == b"\x1f\x8b"
