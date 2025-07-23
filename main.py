from pyspark.sql.functions import col
from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

spark = SparkSession.builder \
    .appName("TestApp") \
    .getOrCreate()


def Mensagem():
    return StructType([
        StructField("mensagem", StringType()),
        StructField("nome", StringType()),
        StructField("idade", IntegerType()),
        StructField("cidade", StringType()),
        StructField("data_envio", StringType()),
        StructField("hora_envio", StringType()),
        StructField("dispositivo", StringType()),
        StructField("tipo_mensagem", StringType()),
        StructField("comprimento_mensagem", StringType()),
        StructField("grupo", BooleanType()),
    ])


df = spark.read \
    .schema(Mensagem()) \
    .option("multiLine", "true") \
    .json('mensagens.json')

df.filter(col("comprimento_mensagem") > 2).show()
