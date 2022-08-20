import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as functions
from pyspark.sql.functions import regexp_replace, when, year, month, to_date, col
from pyspark.sql.types import StringType, IntegerType, FloatType
import re

def start_or_create_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession
             .builder
             .appName("Processamento de Dados de Gasolina no Brasil")
             .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar')
             .getOrCreate()
             )
    return spark


def rename_columns(dataframe):
    
    dataframe= dataframe.select([functions.col(x).alias(x.lower()) for x in dataframe.columns])
    dataframe = dataframe.select([functions.col(col).alias(re.sub(" -","",col)) for col in dataframe.columns])
    dataframe = dataframe.select([functions.col(col).alias(re.sub(" ","_",col)) for col in dataframe.columns])
    
    return dataframe

def add_year(dataframe, coluna):
    
    dataframe = dataframe.withColumn("data", to_date(col(coluna),"dd/MM/yyyy"))
    dataframe = dataframe.withColumn("ano", year(col("data")))
    
    return dataframe

def add_semestre(dataframe, coluna):
    
    
    dataframe = dataframe.withColumn('semestre',month(coluna))
    dataframe = dataframe.withColumn('semestre',when(col("semestre") < 7, 1)
                                                .when(col("semestre") >= 7, 2))
    
    return dataframe

def add_filename_input(dataframe):
    
    
    dataframe = dataframe.withColumn("input_file_name", functions.input_file_name())
    
    return dataframe

def put_file_gcs(dataframe,path_output,formato):
    
    dataframe.repartition(1).write.format(formato).mode("overwrite").save(path_output)
    
    return None

def write_bigquery(dataframe, bq_dataset, bq_table, gcs_tmp_bucket):
    
    #spark.conf.set('temporaryGcsBucket', gcs_tmp_bucket)
    dataframe.write \
        .format("bigquery") \
        .option("table","{}.{}".format(bq_dataset, bq_table)) \
        .option("temporaryGcsBucket", gcs_tmp_bucket) \
        .mode('append') \
        .save()

    return None


def main(path_input, path_output, file_format, bq_dataset, bq_table, gcs_tmp_bucket):
    try:
        
        spark = start_or_create_spark()
        df = spark.read.format('csv').option("header", "true").option('delimiter',';').load(path_input)
        df = rename_columns(df)
        df = add_year(df,"data_da_coleta")
        df = add_semestre(df,"data")
        df = add_filename_input(df)
        df = df.withColumn("numero_rua",col("numero_rua").cast(IntegerType())) \
               .withColumn("ano",col("ano").cast(StringType())) \
               .withColumn("semestre",col("semestre").cast(StringType())) \
               .withColumn('valor_de_venda', regexp_replace('valor_de_venda', ',', '.').cast(FloatType())) \
               .withColumn('valor_de_compra', regexp_replace('valor_de_compra', ',', '.').cast(FloatType())) 
        put_file_gcs(df, path_output, file_format)
        write_bigquery(df, bq_dataset, bq_table, gcs_tmp_bucket)
       
        return df
    except Exception as ex:
        print(ex)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--path_input',
        type=str,
        dest='path_input',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--path_output',
        type=str,
        dest='path_output',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--file_format',
        type=str,
        dest='file_format',
        required=True,
        help='Type format save file')

    parser.add_argument(
        '--bq_dataset',
        type=str,
        dest='bq_dataset',
        required=True,
        help='Dataset do BQ')

    parser.add_argument(
        '--table_bq',
        type=str,
        dest='table_bq',
        required=True,
        help='Tabela do BigQuery Destino')

    known_args, pipeline_args = parser.parse_known_args()

    main(path_input=known_args.path_input,
               path_output=known_args.path_output,
               file_format=known_args.file_format,
               bq_dataset=known_args.bq_dataset,
               bq_table=known_args.table_bq,
               gcs_tmp_bucket="bucket_staging_para_dataproc"
         )

