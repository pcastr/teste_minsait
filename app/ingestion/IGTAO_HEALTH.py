from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Inicializa a Spark Session com suporte a Delta Lake
builder = SparkSession.builder.appName("FHIR_to_Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define o caminho dos arquivos JSON e o destino Delta Lake
json_path = "../../data/raw"  # Exemplo: "/data/fhir"
delta_path = "../../data/bronze"  # Exemplo: "/data/delta_fhir"

# Lê os arquivos JSON para um DataFrame
df = spark.read.option("multiline", "true").json(json_path)

# Mostra a estrutura do JSON para verificar o formato
df.printSchema()

# Salva os dados no formato Delta Lake
df.write.format("delta").mode("overwrite").save(delta_path)

print(f"Dados FHIR convertidos para Delta Lake em: {delta_path}")

# Finaliza a sessão
spark.stop()

