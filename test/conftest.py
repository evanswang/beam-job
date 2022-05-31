import pytest
import os


root_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="function")
def create_test_output_path():
    test_output_path = os.path.join(root_dir, "output")
    if not os.path.exists(test_output_path):
        os.mkdir(test_output_path)


@pytest.fixture(scope="function")
def get_test_output_path(create_test_output_path):
    test_output_path = os.path.join(root_dir, "output")
    return test_output_path


@pytest.fixture(scope="function")
def get_example_data_path(create_test_output_path):
    example_data_path = os.path.join(root_dir, "example_data")
    return example_data_path
