"""
Stock Analytics Beam Pipeline

This script defines the main Apache Beam pipeline for processing
real-time stock ticks into 1-minute OHLCV bars.
"""

import argparse
import logging
import json
from typing import List, Tuple, Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window


class OhlcvCombineFn(beam.CombineFn):
    """
    Core aggregation logic to combine a window of ticks into an OHLCV bar.
    
    A "tick" is a tuple: (timestamp, price, volume)
    """

    def create_accumulator(self) -> List:
        # The accumulator is a list of all ticks in the window
        return []

    def add_input(self, accumulator: List, input_tick: Tuple) -> List:
        # Add the tick (timestamp, price, volume) to the list
        accumulator.append(input_tick)
        return accumulator

    def merge_accumulators(self, accumulators: List[List]) -> List:
        # Combine multiple partial lists into one large list for final processing
        return [tick for acc in accumulators for tick in acc]

    def extract_output(self, accumulator: List) -> Dict:
        # All ticks for one symbol in a 60-second window are now in one list.
        # Sort by timestamp to find open and close.
        sorted_ticks = sorted(accumulator, key=lambda x: x[0])
        
        # Calculate OHLCV
        open_price = sorted_ticks[0][1]
        high_price = max(tick[1] for tick in sorted_ticks)
        low_price = min(tick[1] for tick in sorted_ticks)
        close_price = sorted_ticks[-1][1]
        total_volume = sum(tick[2] for tick in sorted_ticks)

        return {
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': total_volume,
        }


def run(project_id: str, topic_name: str, table_spec: str, pipeline_args: List[str]):
    """
    Constructs and runs the Apache Beam pipeline.
    """
    
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:

        # --- Step 1: Read from Pub/Sub ---
        ticks = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                topic=f"projects/{project_id}/topics/{topic_name}"
            )
            | 'DecodeMessage' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        )

        # --- Step 2: Windowing & Aggregation ---
        ohlcv_bars = (
            ticks
            # We must provide the timestamp for Beam to track windows
            | 'AddTimestamp' >> beam.Map(lambda tick: beam.window.TimestampedValue(tick, tick['timestamp']))
            # Group into 60-second tumbling windows
            | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))
            # Map into a (Key, Value) pair for aggregation
            # Key: stock symbol. Value: (timestamp, price, volume)
            | 'MapToKV' >> beam.Map(lambda tick: (
                tick['symbol'],
                (tick['timestamp'], tick['last_price'], tick['volume'])
            ))
            # Combine all ticks for each key (symbol) within the window
            | 'CalculateOHLCV' >> beam.CombinePerKey(OhlcvCombineFn())
        )

        # --- Step 3: Format for BigQuery ---
        output_table = (
            ohlcv_bars
            | 'FormatForBigQuery' >> beam.Map(
                lambda kv, win=beam.DoFn.WindowParam: {
                    'symbol': kv[0],
                    'window_start': win.start.to_rfc3339(), # Get window start time
                    'open': kv[1]['open'],
                    'high': kv[1]['high'],
                    'low': kv[1]['low'],
                    'close': kv[1]['close'],
                    'volume': kv[1]['volume'],
                }
            )
        )

        # --- Step 4: Write to BigQuery ---
        output_table | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=table_spec,
            # Schema will match the dictionary keys
            schema='SCHEMA_AUTODETECT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            # We will create the table ourselves, so this is safer
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )


def main():
    """Main entry point: parses arguments and runs the pipeline."""
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument(
        '--topic_name', default='stock-ticks', help='Pub/Sub topic to read from'
    )
    parser.add_argument(
        '--dataset_id', default='stock_analytics', help='BigQuery dataset ID'
    )
    parser.add_argument(
        '--table_name', default='ohlcv_1min', help='BigQuery table name'
    )

    # parse_known_args splits args into those we defined
    # and those Beam needs (like --runner, --streaming, etc.)
    known_args, pipeline_args = parser.parse_known_args()

    table_spec = f"{known_args.project_id}:{known_args.dataset_id}.{known_args.table_name}"

    run(
        project_id=known_args.project_id,
        topic_name=known_args.topic_name,
        table_spec=table_spec,
        pipeline_args=pipeline_args,
    )


if __name__ == '__main__':
    main()