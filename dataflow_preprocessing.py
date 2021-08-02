import io
import re
import csv
import argparse
import logging

import pandas as pd

from google.cloud import storage

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://input-bucket-2/AB_NYC_2019.csv',
      help='Input file to process.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    processed_data = (
                p 
                | beam.Create([known_args.input])
                | beam.FlatMap(create_dataframe)
             ) 

def create_dataframe(readable_file):
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    csv_dict = csv.DictReader(io.TextIOWrapper(gcs_file))
    df = pd.DataFrame(csv_dict) 
    logging.info(df.shape)
    df.drop(['name'], axis = 1, inplace = True)
    df.drop(['neighbourhood_group'], axis = 1, inplace = True)
    df.drop(['host_name'], axis = 1, inplace = True)
    logging.info(df.columns)
    client = storage.Client()
    bucket = client.get_bucket('input-bucket-2')
    bucket.blob('processed_data.csv').upload_from_string(df.to_csv(index=False, header=False), 'text/csv')

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
