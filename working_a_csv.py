import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


input_file = '/Users/ramon/beam/purchase.csv'  # Replace with the actual path to your input file
output_file = '/Users/ramon/beam/output.json'  # Replace with the desired path for the output file

cols = ["data", "id", "name", "value"]

pcol_fluxo = (
    pipeline
    | "ler arquivo" >> ReadFromText(input_file, skip_header_lines=1)
    | "definindo as colunas" >> beam.Map(lambda x: x.split(";"))
    | "transformando list em dict" >> beam.Map(lambda x: dict(zip(cols,x)))
    | "obtem o dia do campo data" >> beam.Map(lambda x: {**x, 'day': x['data'].split("-")[2]})
    | "obtem ano-mes do campo data" >> beam.Map(lambda x: {**x, 'year_month': "-".join(x['data'].split("-")[:2])})
    | "pega o id e cria uma chave pro json" >> beam.Map(lambda x: (x['id'], x)) 
    | "Agrupar por uid" >> beam.GroupByKey()
    | "printar" >> beam.Map(print)
    #| 'WriteToText' >> beam.io.WriteToText(output_file)
)

pipeline.run()
