#parquet file needs a schema to use that format.

import pyarrow

schema = [('uf', pyarrow.string()),
          ('ano', pyarrow.int32()),
          ('mes', pyarrow.int32()),
          ('chuvas', pyarrow.float32()),
          ('dengue', pyarrow.float32())
          ]


resultado = (
  pipeline
    | 'Persistir em parquet ' >> beam.io.WriteToParquet('resultado_parquet',
        file_name_suffix=".parquet",
        schema=pyarrow.schema(schema)
    )
