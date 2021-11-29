from datetime import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, DateType
import pyspark.sql.functions as funcs
from pyspark.sql.functions import sha2, concat_ws
import logging

parser = argparse.ArgumentParser(description='Ingestão E=commerce')
parser.add_argument('-sp', '--save_path',
                    dest='save_path', type=str)

args = parser.parse_args()
save_path = args.save_path

spark = SparkSession.builder \
    .master("local") \
    .appName("Ingestão E-Commerce") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

schema_ecomerce = StructType([
    StructField("InvoiceNo", IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])


def read_file_csv(schema_ecomerce):
    logging.info("Lendo arquivo csv")
    return spark.read.schema(schema_ecomerce).csv("/Users/u007654/Documents/karol/dados/data_pipelines/blob/data.csv", header=True)


def convert_date(data):
    dt_convert = datetime.strptime(data, '%d/%m/%Y')
    return dt_convert


def transformation_df(df):
    logging.info("Transformando dados")
    convert_date_udf = funcs.udf(convert_date, DateType())
    transformated_df = df.withColumn(
        "InvoiceDate", convert_date_udf(df.InvoiceDate))

    df = df.withColumn("CHAVEREGISTRO", sha2(
        concat_ws("||", *df.columns), 256)).show(truncate=False)

    return transformated_df


def write_df(df, save_path):
    try:
        logging.info("Verificando chave de registro e salvando DF em PARQUET")
        df_rw = spark.read.load(save_path)
        df_write = df.join(df_rw, 'CHAVEREGISTRO', 'leftanti')
        df_write.write.mode('append').fomart('parquet').option(
            'mergeSchema', 'true').partitionBy('InvoiceDate').save(save_path)
        logging.info("Execuçãp finalizada com sucesso!")
    except Exception as e:
        logging.info(f'Ocorreu a seguinte exception: {e}')
        logging.info('Não tem dado criado ainda, criando...')
        df.write.mode('overwrite').format('parquet').option(
            'mergeSchema', 'true').partitionBy('InvoiceDate').save(save_path)
        logging.info("Execuçãp finalizada com sucesso!")


def main():
    df = read_file_csv(schema_ecomerce)
    df = transformation_df(df)
    write_df(df, save_path)


if __name__ == "__main__":
    main()
    spark.stop
