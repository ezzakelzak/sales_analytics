import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")
DATASET = os.getenv("DATASET")
TABLE = os.getenv("TABLE")
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")
TEMP_BUCKET = os.getenv("TEMP_BUCKET")
STAGING_BUCKET = os.getenv("STAGING_BUCKET")
REGION = os.getenv("REGION")

options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID,
    region=REGION,
    temp_location=TEMP_BUCKET,
    staging_location=STAGING_BUCKET,
    job_name='sales-pubsub-to-bq',
    service_account_email=SERVICE_ACCOUNT_EMAIL, 
    num_workers=1,
    max_num_workers=3,
    streaming=True
)
options.view_as(StandardOptions).streaming = True

def parse_event(message):
    try:
        event = json.loads(message.decode('utf-8'))
        if not all(k in event for k in ['event_id', 'event_timestamp', 'sale', 'customer', 'items']):
            return None
        rows = []
        for item in event['items']:
            row = {
                "event_id": event['event_id'],
                "event_timestamp": event['event_timestamp'],
                "sale_id": event['sale'].get('sale_id'),
                "sale_timestamp": event['sale'].get('sale_timestamp'),
                "store_id": event['sale'].get('store_id'),
                "channel": event['sale'].get('channel'),
                "payment_method": event['sale'].get('payment_method'),
                "currency": event['sale'].get('currency'),
                "total_amount": event['sale'].get('total_amount'),
                "customer_id": event['customer'].get('customer_id'),
                "loyalty_level": event['customer'].get('loyalty_level'),
                "age": event['customer'].get('age'),
                "country": event['customer'].get('country'),
                "product_id": item.get('product_id'),
                "product_name": item.get('product_name'),
                "quantity": item.get('quantity'),
                "unit_price": item.get('unit_price'),
                "category": item.get('category')
            }
            rows.append(row)
        return rows
    except Exception as e:
        print(f"Error parsing event: {e}")
        return None

with beam.Pipeline(options=options) as p:
    (
        p
        | ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | beam.FlatMap(parse_event)
        | WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
            schema={
                "fields": [
                    {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "event_timestamp", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "sale_id", "type": "STRING"},
                    {"name": "sale_timestamp", "type": "STRING"},
                    {"name": "store_id", "type": "STRING"},
                    {"name": "channel", "type": "STRING"},
                    {"name": "payment_method", "type": "STRING"},
                    {"name": "currency", "type": "STRING"},
                    {"name": "total_amount", "type": "FLOAT"},
                    {"name": "customer_id", "type": "INTEGER"},
                    {"name": "loyalty_level", "type": "STRING"},
                    {"name": "age", "type": "INTEGER"},
                    {"name": "country", "type": "STRING"},
                    {"name": "product_id", "type": "STRING"},
                    {"name": "product_name", "type": "STRING"},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "unit_price", "type": "FLOAT"},
                    {"name": "category", "type": "STRING"}
                ]
            },
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )
    )
