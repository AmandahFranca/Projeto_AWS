from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *

# Inicializa o Spark e o Glue Context
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init("nome_do_job", {})

# Caminho do arquivo CSV na S3
source_file = "s3://data-lake-ahelena/Raw/Local/CSV/movies/2024/07/11/movies.csv"

# Leitura do arquivo CSV com delimitador pipe e transformação de contexto
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="csv",
    format_options={"withHeader": True, "separator": "|"},  # Ajusta o delimitador para pipe
    transformation_ctx="leitura_csv"
).toDF()

for column in df.columns:
    df = df.withColumn(column, trim(col(column)))

# Preencher valores NaN na coluna 'genero' com ''
df = df.fillna({'genero': ''})

# Filtrar filmes do gênero 'Comedy' e artistas específicos
df_comedy = df.filter(col('genero').rlike('Comedy')) \
              .filter(col('nomeArtista').isin('Adam Sandler', 'Jim Carrey'))

# Selecionar as colunas relevantes
df_comedy = df_comedy.select(
    col('id'),
    col('genero'),
    col('numeroVotos'),
    col('anoLancamento'),
    col('notaMedia'),
    col('titulosMaisConhecidos'),
    col('nomeArtista')  # Inclua esta coluna se for relevante
)

# Aplicar filtros adicionais
df_comedy_filtered = df_comedy.filter(col('notaMedia') > 0) \
                          
# Exibir algumas linhas para verificação
df_comedy_filtered.show(5, truncate=False)

# Caminho de destino para o arquivo Parquet
destination_path =  "s3://data-lake-ahelena/Trusted_zone/teste/"

# Gravação do DataFrame como Parquet
df_comedy_filtered.write.mode("overwrite").parquet(destination_path)

# Finaliza o job Glue
job.commit()

