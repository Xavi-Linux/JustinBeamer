"""
    Serverless Data Processing with Dataflow - Branching from
    Serverless Data Processing with Dataflow: Develop Pipelines
"""
import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes


def parse_json(element):
    return json.loads(element)


def drop_fields(element):
    element.pop('user_agent')
    return element

# ### main


def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--inputPath', required=True, help='Specify the input path')
    parser.add_argument('--outputPath', required=True, help='Specify the output path')
    parser.add_argument('--tableName', required=True, help='Specify the BigQuery table name')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    # argumnent parser

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "ip",
                "type": "STRING"
            },
            {
                "name": "user_id",
                "type": "STRING"
            },
            {
                "name": "lat",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "lng",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "timestamp",
                "type": "STRING"
            },
            {
                "name": "http_request",
                "type": "STRING"
            },
            {
                "name": "http_response",
                "type": "INTEGER"
            },
            {
                "name": "num_bytes",
                "type": "INTEGER"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    '''

    Steps:
    1) Read something
    2) Transform something
    3) Write something

    '''

    text = p | 'ReadFromGCS' >> beam.io.ReadFromText(opts.inputPath)

    (text
        | 'ParseJson' >> beam.Map(parse_json)
        | 'DropFields' >> beam.Map(drop_fields)
        | 'FilterFn' >> beam.Filter(lambda element: element['num_bytes'] > 120)
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            opts.tableName,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
     )

    (text
        | beam.io.WriteToText(file_path_prefix=opts.outputPath)
     )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == '__main__':
  run()
