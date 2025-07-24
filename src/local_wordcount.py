import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class CountWords(beam.PTransform):
    def expand(self, pcoll):
        # 1. Split each line into words
        words = pcoll | 'SplitIntoWords' >> beam.FlatMap(lambda line: line.split())

        # 2. Count occurrences of each word
        counted_words = words | 'CountOccurrences' >> beam.combiners.Count.PerElement()

        return counted_words

def run_pipeline():

    pipeline_options = PipelineOptions()

    # Create a pipeline object
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Define your input data (a PCollection)
        lines = pipeline | 'Create' >> beam.Create([
            'hello world',
            'hello python',
            'world beam',
            'hello'
        ])

        # Apply the custom PTransform
        output = lines | 'CountWordsTransform' >> CountWords()

        # Define how to process the output (e.g., print to console)
        output | 'PrintResults' >> beam.Map(print)

if __name__ == '__main__':
    print("Running Apache Beam pipeline locally...")
    run_pipeline()
    print("Pipeline finished.")