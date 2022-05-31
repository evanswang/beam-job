import gzip
import shutil
from beam_job.settings import (
    OUTPUT_FILE_NAME,
    OUTPUT_FILE_SUFFIX,
)
from os import walk
import os


def compress_files(input_folder: str, output_file: str):
    with gzip.open(output_file, "wb") as f_out:
        for (dirpath, dirnames, filenames) in walk(input_folder):
            for filename in filenames:
                is_result_file = filename.startswith(
                    OUTPUT_FILE_NAME
                ) and filename.endswith(OUTPUT_FILE_SUFFIX)
                if not is_result_file:
                    continue
                file_path = os.path.join(dirpath, filename)
                with open(file_path, "rb") as f_in:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(file_path)
