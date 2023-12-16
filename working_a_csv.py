import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


input_file = '/purchase.csv'  # Replace with the actual path to your input file
output_file = '/output.json'  # Replace with the desired path for the output file


cols = ["data", "id", "name", "value"]


def valor_por_id(item):
    ide, dic = item
    for i in dic:
        if bool(re.search(r'\d', i['value'])):
            yield ("%s-%s" %(ide, i['year_month']), float(i['value']))
        else:
            yield ("%s-%s" %(ide, i['year_month']), 0.0)

pcol_fluxo = (
    pipeline
    | "ler arquivo" >> ReadFromText(input_file, skip_header_lines=1)
    | "definindo as colunas" >> beam.Map(lambda x: x.split(";"))
    | "transformando list em dict" >> beam.Map(lambda x: dict(zip(cols,x)))
    | "obtem o dia do campo data" >> beam.Map(lambda x: {**x, 'day': x['data'].split("-")[2]})
    | "obtem ano-mes do campo data" >> beam.Map(lambda x: {**x, 'year_month': "-".join(x['data'].split("-")[:2])})
    | "pega o id e cria uma chave pro json" >> beam.Map(lambda x: (x['id'], x)) 
    | "Agrupar por uid" >> beam.GroupByKey()
    | "yield in function need use flatmap instead map" >> beam.FlatMap(valor_por_id)
    | "somar valores" >> beam.CombinePerKey(sum) 
    | "printar" >> beam.Map(print)
    #| 'WriteToText' >> WriteToText(output_file)
)


pipeline.run()
