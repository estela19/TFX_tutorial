import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# text file is saved in google cloud storage bucuket
input_file = "gs://dataflow-samples/shakespeare/kinglear.txt"
output_file = "/mnt/c/github/TFXtutorial/chapter02/output.txt"

# define pipeline options object
pipeline_options = PipelineOptions()

# Setup apache-beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
	# read text file or transform file pattern to pcollection
	# read text and make data collection
	lines = p | ReadFromText(input_file)

	# transform at collection
	counts = (
		lines
		| 'Split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
		| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
		| 'GroupAndSum' >> beam.CombinePerKey(sum))
	
	# transform string and save at pcollection how many word appear
	def format_result(word_count):
		(word, count) = word_count
		return "{}: {}".format(word, count)

	output = counts | 'Format' >> beam.Map(format_result)

	# show result with "Write" command
	output | WriteToText(output_file)

