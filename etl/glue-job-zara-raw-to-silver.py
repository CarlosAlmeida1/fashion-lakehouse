import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1 INR em BRL (cotacao referencia: ~0.054)
TAXA_CONVERSAO_INR_BRL = 0.054

S3_RAW_INPUT = "s3://dominio-mercado-zara-project/raw/*/*.csv"
S3_SILVER_OUTPUT = "s3://dominio-mercado-zara-project/processed/zara/"

df_raw = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv(S3_RAW_INPUT)

# Extracao de metadados do arquivo
df_with_meta = df_raw.withColumn("file_path", F.input_file_name()) \
    .withColumn("genero_en", F.regexp_extract(F.col("file_path"), r"/(Men|Women)/", 1)) \
    .withColumn("categoria_macro_en", F.regexp_extract(F.col("file_path"), r"/([^/]+)\.csv$", 1))

# Ignora cabeçalhos duplicados
df_cleaned = df_with_meta.filter(F.lower(F.col("product_name")) != "product_name")

# 1. Limpeza de Preco Original (INR) e Conversao para Real (BRL)
df_cleaned = df_cleaned.withColumn(
    "preco_inr",
    F.regexp_replace(F.col("price"), r"[₹\s,]", "").cast(DecimalType(10, 2))
)

df_cleaned = df_cleaned.withColumn(
    "preco_brl",
    F.round(F.col("preco_inr") * F.lit(TAXA_CONVERSAO_INR_BRL), 2).cast(DecimalType(10, 2))
)

# 2. Localizacao de Genero
df_cleaned = df_cleaned.withColumn(
    "genero",
    F.when(F.col("genero_en") == "Men", "Masculino")
     .when(F.col("genero_en") == "Women", "Feminino")
     .otherwise("Unissex")
)

# 3. Localizacao de Categorias
df_cleaned = df_cleaned.withColumn(
    "categoria_macro",
    F.when(F.col("categoria_macro_en") == "SHOES", "Calcados")
     .when(F.col("categoria_macro_en") == "JEANS", "Jeans")
     .when(F.col("categoria_macro_en") == "SHIRTS", "Camisas")
     .when(F.col("categoria_macro_en") == "T-SHIRTS", "Camisetas")
     .when(F.col("categoria_macro_en") == "TROUSERS", "Calcas")
     .when(F.col("categoria_macro_en") == "JACKETS", "Jaquetas")
     .when(F.col("categoria_macro_en") == "BLAZERS", "Blazers")
     .when(F.col("categoria_macro_en") == "DRESSES", "Vestidos")
     .when(F.col("categoria_macro_en") == "SKIRTS", "Saias")
     .when(F.col("categoria_macro_en") == "BAGS", "Bolsas")
     .otherwise(F.col("categoria_macro_en"))
)

df_cleaned = df_cleaned.withColumn("details_lower", F.lower(F.coalesce(F.col("details"), F.lit(""))))

# 4. Mineracao e Traducao de Materiais
df_silver = df_cleaned.withColumn(
    "material_predominante",
    F.when(F.col("details_lower").rlike(r"\bleather\b"), "Couro")
     .when(F.col("details_lower").rlike(r"\bsuede\b"), "Camurca")
     .when(F.col("details_lower").rlike(r"\bdenim\b"), "Denim/Jeans")
     .when(F.col("details_lower").rlike(r"\blinen\b"), "Linho")
     .when(F.col("details_lower").rlike(r"\bcotton\b"), "Algodao")
     .when(F.col("details_lower").rlike(r"\bknit\b"), "Trico/Malha")
     .when(F.col("details_lower").rlike(r"\bcanvas\b"), "Lona")
     .when(F.col("details_lower").rlike(r"\bjute\b"), "Juta")
     .otherwise("Sintetico / Outros")
)

# 5. Mineracao e Traducao de Solado / Modelagem
df_silver = df_silver.withColumn(
    "estilo_solado",
    F.when(F.col("details_lower").rlike(r"track sole"), "Tratorado")
     .when(F.col("details_lower").rlike(r"chunky"), "Chunky/Robusto")
     .when(F.col("details_lower").rlike(r"vibram"), "Vibram")
     .otherwise("Padrao")
)

colunas_selecionadas = [
    "genero",
    "categoria_macro",
    "product_name",
    "preco_inr",
    "preco_brl",
    "material_predominante",
    "estilo_solado",
    "link",
    "details"
]

df_final = df_silver.select(colunas_selecionadas)

# 6. Gravacao particionada em portugues
df_final.write \
    .mode("overwrite") \
    .partitionBy("genero", "categoria_macro") \
    .parquet(S3_SILVER_OUTPUT)

job.commit()