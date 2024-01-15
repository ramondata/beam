import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='cloud-treino',
    job_name='bq-pipe',
    temp_location='gs://purchase-stock-beam/temp',
    region='southamerica-east1')
pipeline = beam.Pipeline(options=beam_options)


max_temperatures = (
    pipeline
    | 'QueryTableStdSQL' >> ReadFromBigQuery(
        query='SELECT * FROM '\
              '`cloud-treino.shop.purchase`',
        use_standard_sql=True)
    | "selected one column" >> beam.Map(lambda elem: elem['string_field_0'])
    | "separar por colunas" >> beam.Map(lambda x: x.split(";"))
    | "escrever arquivo" >> WriteToText('gs://purchase-stock-beam/from_bq', file_name_suffix=".csv")
    )


pipeline.run()
