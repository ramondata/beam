import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='cloud-treino',
    job_name='testing-pipe',
    temp_location='gs://purchase-stock-beam/temp',
    region='southamerica-east1')
pipeline = beam.Pipeline(options=beam_options)


output_file = 'gs://purchase-stock-beam/result'  # Replace with the desired path for the output file


def valor_por_id(item):
    ide, dic = item
    for i in dic:
        if bool(re.search(r'\d', i['value'])):
            yield ("%s-%s" %(ide, i['year_month']), float(i['value']))
        else:
            yield ("%s-%s" %(ide, i['year_month']), 0.0)


def filtra(x):
    id, dicionario = x
    if any([dicionario['pcol_purchase'] == [], dicionario['pcol_stock'] == []]):
        return False
    else:
        return True 


def teste_filtro(x):
    id, dic = x
    if dic["pcol_purchase"] < dic["pcol_stock"]:
        return True
    return False


def tuple_to_csv_format_string(x):
    a,b,c = x
    y = (str(a), str(b), str(c))
    return ';'.join(y) 


pcol_fluxo = (
    pipeline
    | "ler arquivo" >> ReadFromText("gs://purchase-stock-beam/purchase.csv", skip_header_lines=1)
    | "definindo as colunas" >> beam.Map(lambda x: x.split(";"))
    | "transformando list em dict" >> beam.Map(lambda x: dict(zip(["data", "id", "name", "value"],x)))
    | "obtem o dia do campo data" >> beam.Map(lambda x: {**x, 'day': x['data'].split("-")[2]})
    | "obtem ano-mes do campo data" >> beam.Map(lambda x: {**x, 'year_month': "-".join(x['data'].split("-")[:2])})
    | "pega o id e cria uma chave pro json" >> beam.Map(lambda x: (x['id'], x)) 
    | "Agrupar por uid" >> beam.GroupByKey()
    | "yield in function need use flatmap instead map" >> beam.FlatMap(valor_por_id)
    | "somar valores" >> beam.CombinePerKey(sum) 
    #| "printar" >> beam.Map(print)
)

pcol_second = (
    pipeline
    | "Leitura do segundo pcol" >> ReadFromText('gs://purchase-stock-beam/stock.csv', skip_header_lines=1)
    | "separando em colunas" >> beam.Map(lambda x: x.split(";"))
    | "Chave e preço" >> beam.Map(lambda x: ("%s-%s" %(x[1], '-'.join(x[0].split("-")[:2])), float(x[4])))
    | "Agrupar por chave" >> beam.CombinePerKey(sum)
    #| "Printa o resultado" >> beam.Map(print)

)

resultado = (
    #(pcol_fluxo, pcol_second)
    #| "empilha as pcolections" >> beam.Flatten()
    #| beam.GroupByKey()
    ({"pcol_purchase": pcol_fluxo, "pcol_stock": pcol_second})
    | "merge" >> beam.CoGroupByKey()
    | "filtrar dados limpeza" >> beam.Filter(filtra)
    | "filtrar dados comparando" >> beam.Filter(teste_filtro)
    | "layout de tupla de volta" >> beam.Map(lambda x: (x[0], x[1]['pcol_purchase'][0], x[1]['pcol_stock'][0]))
    | "formato de csv" >> beam.Map(tuple_to_csv_format_string)
    | 'WriteToText' >> WriteToText(output_file, file_name_suffix='csv', num_shards=1)
    #| beam.Map(print)
)


#beam.combiners.Count.PerKey()
#beam.combiners.Count.Globally()
#beam.combiners.Count.PerElement()

pipeline.run()
