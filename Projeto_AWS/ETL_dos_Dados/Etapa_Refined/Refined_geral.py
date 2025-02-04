import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Definindo o id autoincremental
def add_auto_increment_id(df, id_column_name):
    return df.withColumn(id_column_name, monotonically_increasing_id())

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dest_path = "s3://data-lake-ahelena/Refined/"

# Leitura dos dados JSON em formato Parquet
df_json1 = spark.read.parquet("s3://data-lake-ahelena/Trusted_zone/dt=2024-08-16/*")
df_json2 = spark.read.parquet("s3://data-lake-ahelena/Trusted_zone/dt=2024-08-29/*")

# Ajustar os schemas dos DataFrames para unir os dados
for column in [column for column in df_json2.columns if column not in df_json1.columns]:
    df_json1 = df_json1.withColumn(column, lit(None))

for column in [column for column in df_json1.columns if column not in df_json2.columns]:
    df_json2 = df_json2.withColumn(column, lit(None))

merged_df = df_json1.unionByName(df_json2)

# DataFrame correspondente à dimensão orçamento
dim_orcamento = merged_df.select(
    col('imdb_id').alias('id_filme'),
    coalesce(col('orcamento'), lit(0)).alias('orcamento')
).dropDuplicates()
dim_orcamento = add_auto_increment_id(dim_orcamento, 'orcamento_id')
dim_orcamento.write.format('parquet').mode('overwrite').save(f'{dest_path}/dim_orcamento')

# DataFrame correspondente à dimensão receita
dim_receita = merged_df.select(
    col('imdb_id').alias('id_filme'),
    coalesce(col('receita'), lit(0)).alias('receita')
).dropDuplicates()
dim_receita = add_auto_increment_id(dim_receita, 'receita_id')
dim_receita.write.format('parquet').mode('overwrite').save(f'{dest_path}/dim_receita')

# DataFrame correspondente à dimensão popularidade
dim_popularidade = merged_df.select(
    col('imdb_id').alias('id_filme'),
    coalesce(col('popularidade'), lit(None)).alias('popularidade')
).dropDuplicates()
dim_popularidade = add_auto_increment_id(dim_popularidade, 'popularidade_id')
dim_popularidade.write.format('parquet').mode('overwrite').save(f'{dest_path}/dim_popularidade')

# DataFrame correspondente à tabela fato filme
fato_filme = merged_df.select(
    col('imdb_id'),
    coalesce(col('titulo'), col('titulo')).alias('titulo')
)

# Adiciona os detalhes das dimensões à tabela fato_filme
fato_filme = fato_filme.join(
    dim_orcamento, 
    fato_filme.imdb_id == dim_orcamento.id_filme, 
    'left'
).drop('id_filme')

fato_filme = fato_filme.join(
    dim_receita, 
    fato_filme.imdb_id == dim_receita.id_filme, 
    'left'
).drop('id_filme')

fato_filme = fato_filme.join(
    dim_popularidade, 
    fato_filme.imdb_id == dim_popularidade.id_filme, 
    'left'
).drop('id_filme')

# Salva a tabela fato no formato Parquet
fato_filme.write.format('parquet').mode('overwrite').save(f'{dest_path}/fato_filme')

job.commit()
