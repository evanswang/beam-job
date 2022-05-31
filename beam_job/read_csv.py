import apache_beam as beam
from apache_beam.io.filesystems import FileSystems as beam_fs
import codecs
import csv
from typing import Dict, Iterable, List


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(Dict[str, str])
def ReadCsvFiles(
    pbegin: beam.pvalue.PBegin, file_patterns: List[str]
) -> beam.PCollection[Dict[str, str]]:
    def expand_pattern(pattern: str) -> Iterable[str]:
        for match_result in beam_fs.match([pattern])[0].metadata_list:
            yield match_result.path

    def read_csv_lines(file_name: str) -> Iterable[Dict[str, str]]:
        with beam_fs.open(file_name) as f:
            # Beam reads files as bytes, but csv expects strings,
            # so we need to decode the bytes into utf-8 strings.
            for row in csv.DictReader(codecs.iterdecode(f, "utf-8")):
                yield dict(row)

    return (
        pbegin
        | "Create file patterns" >> beam.Create(file_patterns)
        | "Expand file patterns" >> beam.FlatMap(expand_pattern)
        | "Read CSV lines" >> beam.FlatMap(read_csv_lines)
    )
