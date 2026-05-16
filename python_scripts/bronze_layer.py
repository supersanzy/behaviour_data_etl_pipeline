def run_bronze_layer(spark):

# Creating schema for behaviour datasets
    schema_data = "event_time TIMESTAMP, event_type STRING, product_id INT, category_id LONG, category_code STRING, \
                    brand STRING, price DOUBLE, user_id INT, user_session STRING"

    ## Reading behaviour CSV datasest files into spark dataframe
    oct_2019 = spark.read.schema(schema_data).format("csv").option("header", True).load("/home/supersanzy_de/Downloads/spark_project/data_source_datasets/2019-Oct.csv")

    nov_2019 = spark.read.schema(schema_data).format("csv").option("header", True).load("/home/supersanzy_de/Downloads/spark_project/data_source_datasets/2019-Nov.csv")

    dec_2019 = spark.read.schema(schema_data).format("csv").option("header", True).load("/home/supersanzy_de/Downloads/spark_project/data_source_datasets/2019-Dec.csv")

    jan_2020 = spark.read.schema(schema_data).format("csv").option("header", True).load("/home/supersanzy_de/Downloads/spark_project/data_source_datasets/2020-Jan.csv")

    feb_2020 = spark.read.schema(schema_data).format("csv").option("header", True).load("/home/supersanzy_de/Downloads/spark_project/data_source_datasets/2020-Feb.csv")


    # Merging all the datasets
    behaviour_df = oct_2019.unionByName(nov_2019).unionByName(dec_2019).unionByName(jan_2020).unionByName(feb_2020)

    # Writing the merged datasets to a parquet format and to Bronze Layer
    behaviour_df.write.format("parquet").mode("append").save("/home/supersanzy_de/Downloads/spark_project/medallion_architecture/bronze_layer")
