
# Import Beam Libraries

import apache_beam as beam
import json
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics

# PubSub
from apache_beam.options.pipeline_options import PipelineOptions

def decode_message(element):
    output = element.decode('utf-8')
    return json.loads(output)

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:

    data = (p 
            
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription='projects/awesome-ridge-411708/subscriptions/edem-topic-sub')
            | "Decode_msg" >> beam.Map(decode_message)
            | "Write" >> beam.io.WriteToBigQuery(
                table="awesome-ridge-411708:datasetedem.tabla bla bla",
                schema="Pepe:STRING, Alguilar:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND


            )
    )
    