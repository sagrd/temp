import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from sys import argv

PROJECT_ID = ''
BUCKET_NAME = ''
OUTPUT_FILE = 'output/result.csv'
QUERY = 'select * from bigquery-public-data.london_bicycles.cycle_hire LIMIT 5;'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())
    (p | 'ReadFromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=QUERY, use_standard_sql=True)
       | 'Write PCollection to Bucket' >> WriteToText('gs://{0}/{1}'.format(BUCKET_NAME, OUTPUT_FILE))
    result = p.run()
    result.wait_until_finish()
