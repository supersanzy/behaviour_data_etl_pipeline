from pyspark.sql import SparkSession
# from python_scripts.bronze_layer import run_bronze_layer
# from python_scripts.silver_layer import run_silver_layer
# from python_scripts.gold_layer import run_gold_layer
from python_scripts.load_to_postgres import load_to_postgres


spark = (SparkSession.builder \
         .appName("Spark ETL Pipeline") \
         .getOrCreate()
         )

# run_bronze_layer(spark)
# run_silver_layer(spark)
# run_gold_layer(spark)
load_to_postgres(spark)


spark.stop()