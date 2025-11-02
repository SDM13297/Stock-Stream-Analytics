"""
Unit tests for the stock processing pipeline.
"""

import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam

# Import the class to be tested
from src.stock_pipeline.pipeline import OhlcvCombineFn

def test_ohlcv_combine_fn():
    """
    Tests the OhlcvCombineFn logic with a sample set of ticks.
    """
    
    # A list of ticks for a single window: (timestamp, price, volume)
    TICKS = [
        (100.0, 10.0, 100), # 1. Open
        (110.0, 15.0, 50),  # 2. High
        (120.0, 8.0,  75),  # 3. Low
        (130.0, 12.0, 120), # 4. Close
    ]
    
    # The expected output dictionary
    EXPECTED_OUTPUT = [
        {
            'open': 10.0,
            'high': 15.0,
            'low': 8.0,
            'close': 12.0,
            'volume': 345, # 100 + 50 + 75 + 120
        }
    ]

    # Use Beam's testing pipeline to run the test
    with TestPipeline() as p:
        
        # 1. Create a PCollection of the ticks
        pcoll = p | beam.Create([('NIFTY-50', tick) for tick in TICKS])
        
        # 2. Apply the CombineFn logic
        output = pcoll | beam.CombinePerKey(OhlcvCombineFn())
        
        # 3. Format the output (just get the dictionary)
        formatted_output = output | beam.Map(lambda kv: kv[1])
        
        # 4. Assert that the output matches the expected result
        assert_that(formatted_output, equal_to(EXPECTED_OUTPUT))