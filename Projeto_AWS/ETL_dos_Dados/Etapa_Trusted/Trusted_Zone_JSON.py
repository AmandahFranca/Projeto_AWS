import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType
from pyspark.context import SparkContext


# Inicializar o SparkSession
spark = SparkSession.builder \
    .appName("Transformação TMDb") \
    .getOrCreate()

# Inicializar o contexto do Glue
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Caminho de entrada e saída no S3
source_file = "s3://data-lake-ahelena/Raw/TMDB/JSON/2024/08/16/*"
output_path = "s3://data-lake-ahelena/Trusted_zone/dt=2024-08-16/"

# Leitura do JSON
df_tmdb = spark.read.json(source_file)

# Verifica se o DataFrame está vazio
if df_tmdb.rdd.isEmpty():
    print("O DataFrame está vazio. Verifique o caminho de entrada e o conteúdo dos arquivos JSON.")
else:
    # Verificar o esquema do DataFrame
    df_tmdb.printSchema()
    
    # Remover a coluna 'release_date' se existir
    if "release_date" in df_tmdb.columns:
        df_tmdb = df_tmdb.drop("release_date")

    # Renomear as colunas conforme especificado
    df_tmdb = df_tmdb.withColumnRenamed("Ano", "anoLancamento_tmdb") \
        .withColumnRenamed("Genre_ID", "id_genero") \
        .withColumnRenamed("id", "id_tmdb") \
        .withColumnRenamed("title", "titulo") \
        .withColumnRenamed("popularity", "popularidade") \
        .withColumnRenamed("vote_average", "votacao_media") \
        .withColumnRenamed("vote_count", "contagem_de_voto") \
        .withColumnRenamed("adult", "adulto") \
        .withColumnRenamed("budget", "orcamento") \
        .withColumnRenamed("runtime", "duracao") \
        .withColumnRenamed("revenue", "receita")

    # Converter colunas para tipos adequados e filtrar valores indesejados
    df_tmdb = df_tmdb.withColumn("orcamento", col("orcamento").cast(IntegerType()))
    df_tmdb = df_tmdb.filter(col("orcamento") > 0)

    df_tmdb = df_tmdb.withColumn("receita", col("receita").cast(IntegerType()))
    df_tmdb = df_tmdb.filter(col("receita") > 0)

    df_tmdb = df_tmdb.withColumn("contagem_de_voto", col("contagem_de_voto").cast(IntegerType()))
    df_tmdb = df_tmdb.filter(col("contagem_de_voto") >= 30)

    df_tmdb = df_tmdb.withColumn("duracao", col("duracao").cast(IntegerType()))
    df_tmdb = df_tmdb.filter(col("duracao") >= 80)

    # Verificar se 'imdb_id' existe antes de filtrar
    if "imdb_id" in df_tmdb.columns:
        df_tmdb = df_tmdb.filter(col("imdb_id").isNotNull())

    # Reiniciar o índice com base em 'imdb_id'
    if "imdb_id" in df_tmdb.columns:
        df_tmdb = df_tmdb.withColumn("index", col("imdb_id"))
        df_tmdb = df_tmdb.drop("imdb_id")
        df_tmdb = df_tmdb.withColumnRenamed("index", "imdb_id")
        df_tmdb = df_tmdb.orderBy("imdb_id")

    # Adicionar coluna com a data de extração
    data_extracao = '2024-08-15'  # Corrigido para o formato de data padrão
    df_tmdb = df_tmdb.withColumn("data_coleta_tmdb", lit(data_extracao))

    # Escrever os dados em Parquet, particionado pela coluna 'data_coleta_tmdb'
    df_tmdb.write.partitionBy("data_coleta_tmdb").parquet(output_path, mode="overwrite")

    print(f"Dados processados e salvos com sucesso em {output_path}")

# Encerrar o job do Glue
job.commit()

# Encerrar a sessão do Spark
spark.stop()