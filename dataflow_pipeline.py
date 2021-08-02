import re
import os
import csv
import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

table_spec = 'result.neighborhood_count'
table_schema = 'neighborhood:STRING, count:INTEGER'

class TupToDict(beam.DoFn):
  def process(self, element):
    dictionary = {'neighborhood': element[0],'count': element[1]}
    return [dictionary]

class ExtractNeighborhood(beam.DoFn):
  def process(self, element):
    return [(element[2],1)]

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://input-bucket-2/processed_data.csv',
      help='Input file to process.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    input_data = ( p 
                | 'Read Processed CSV' >> beam.io.ReadFromText(known_args.input)
                | 'Parse file' >> beam.Map(parse_file)
                )

    Neighborhood_data = ( input_data 
                | 'Extract Neighborhood' >> beam.ParDo(ExtractNeighborhood())
                )

    NeighborhoodGroupBy = ( Neighborhood_data
                | 'Group by Neighborhood' >> beam.GroupByKey()
                )

    Count = ( NeighborhoodGroupBy
              | 'Count' >> beam.Map( lambda a : (a[0],len(a[1])))
              )

    output = Count | beam.ParDo(TupToDict())
    output | 'Write to db' >> beam.io.WriteToBigQuery(
                                table_spec,
                                schema=table_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
