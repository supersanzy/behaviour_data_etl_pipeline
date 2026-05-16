from pyspark.sql.functions import *


# READ BRONZE LAYER
def run_silver_layer(spark):
        bronze_layer = spark.read.format("parquet") \
                .load("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/bronze_layer")


        # Data Transformations:
        # Data Enrichment: Replace brand column with same data or N/A values if there's no value
        lookup = bronze_layer.groupBy("category_id") \
        .agg(
                max("category_code").alias("fill_category_code"), \
                max("brand").alias("fill_brand_code"))

        bronze_layer.cache().count()

        # This was an expensive shuffle, so i did broadcast join
        bronze_layer = bronze_layer.join(broadcast(lookup), how="left", on="category_id") \
                .withColumn("category_code", coalesce("fill_category_code", lit("N/A")))\
                .withColumn("brand", coalesce("fill_brand_code", lit("N/A"))) \
                .withColumn("day", dayofmonth("event_time"))\
                .withColumn("month", month("event_time"))\
                .withColumn("year", year("event_time"))\
                .drop("fill_category_code", "fill_brand_code") \
                

        spark.catalog.clearCache()
        # Writing to silver layer in parquet format

        bronze_layer.write.format("parquet").partitionBy("year").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/silver_layer")
