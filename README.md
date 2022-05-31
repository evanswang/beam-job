# beam_job

This repo is created by Shicai Wang for an Apache Beam test.

The code here is tested in Python 3.7.

## Quick Start

### Clone this repo

``` bash
git clone https://github.com/evanswang/beam_job.git
```

### Install requirements.txt

``` bash
cd beam_job
pip3 install -r requirements.txt
```

### Run the code
``` bash
# run tests
pytest test
# run main function
python3 run.sh
```
The output gzip file is in the output/results.jsonl.gz.

## Code structure

### Root level

- run.py: start point of the code
- requirements.txt: required Python packages. Please install them before running.

### Sub-folder beam_job

- composite_transforms.py: new created Composite Transform class for the main logic transformation.
- gz_maker.py: create a gzip file for all the output files.
- main.py: main function of the code
- read_csv: this is the Apache Beam sample code to read CSV file and return PCollection.
- settings: environment variables place
- utils: common tools place

### Sub-folder test

- example_data: sample data for tests
- conftest.py: pytest configurations (fixture)
- test_composite_transforms.py: Apache Beam tests for the new build composite transforms.
- test_gz_maker.py: test the gzip file creation function.
