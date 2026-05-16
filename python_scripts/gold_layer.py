from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Read Silver Layer
def run_gold_layer(spark):
        silver_layer = spark.read.format("parquet").option("header", True)\
                .load("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/silver_layer")


        ###### MODELING THE DATA

        # Users Dimension
        dim_users = silver_layer.select("user_id").distinct() \
                        .withColumn("user_key", row_number().over(Window.orderBy("user_id")))


        # Products Dimension
        window_spec = Window.orderBy("product_id")
        dim_products = silver_layer.select("product_id", "category_id", "category_code", "brand").distinct() \
                        .withColumn("product_key", row_number().over(window_spec))


        # Events Dimension
        dim_events = silver_layer.select("event_type").distinct() \
                        .withColumn("event_key", row_number().over(Window.orderBy("event_type")))


        # Persisting dim_products and dim_events dataframe
        silver_layer.cache().count()


        # Fact Orders
        fact_orders = silver_layer.filter(col("event_type") == "purchase") \
                .withColumn("purchase_date", to_date("event_time")) \
                .withColumn("purchase_time", date_format("event_time", "HH:mm:ss")) \
                .join(broadcast(dim_products), how="inner", on="product_id")\
                .join(broadcast(dim_events), how="inner", on="event_type")\
                .join(dim_users, how="inner", on="user_id") \
                .select("user_key", "product_key", "event_key", "price", "purchase_date", "purchase_time", "month", "day", "year")


        # Fact User Events
        fact_user_events = silver_layer.withColumn("event_date", to_date("event_time")) \
                .withColumn("event_time", date_format("event_time", "HH:mm:ss")) \
                .join(broadcast(dim_products), how="inner", on="product_id")\
                .join(broadcast(dim_events), how="inner", on="event_type")\
                .join(dim_users, how="inner", on="user_id") \
                .select("user_key", "product_key", "event_key", "price", "user_session", "event_date", "event_time", "month", "day", "year")

        spark.catalog.clearCache()



        # SAVING GOLD LAYER IN A PARQUET FORMAT
        dim_users.write.format("parquet").mode("append").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/dim_users")


        dim_products.write.format("parquet").mode("append").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/dim_products")

        dim_events.write.format("parquet").mode("append").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/dim_events")

        fact_orders.write.format("parquet").mode("append").partitionBy("year", "month").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/fact_orders")

        fact_user_events.write.format("parquet").mode("append").partitionBy("year", "month").mode("append") \
                .save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/fact_user_events")
