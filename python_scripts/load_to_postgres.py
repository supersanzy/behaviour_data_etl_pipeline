from dotenv import load_dotenv
from logging_config import setup_logging
import os
import logging

setup_logging()
load_dotenv()

localhost = os.getenv("DBHOST")
dbuser = os.getenv("DBUSER")
dbname = os.getenv("DBNAME")
dbpassword = os.getenv("DBPASSWORD")
port = os.getenv("DBPORT")



jdbc_url = f"jdbc:postgresql://{localhost}:{port}/{dbname}"


# gold_tables =[
#     "dim_users", "dim_products", "dim_events", "fact_orders", "fact_user_events"
# ]

table = "fact_user_events"
file_path = "/home/supersanzy_de/Downloads/spark_project/medallion_architecture/gold_layer/"

# def load_to_postgres(spark):

#     for table in gold_tables:
#         try:
        #     logging.info(f"Starting load for {table}")
        #     df = spark.read.parquet(f"{file_path}/{table}")

        #     df.write \
        #         .format("jdbc") \
        #         .option("url", jdbc_url) \
        #         .option("dbtable", table) \
        #         .option("user", dbuser) \
        #         .option("password", dbpassword) \
        #         .option("driver", "org.postgresql.Driver") \
        #         .mode("append") \
        #         .save()
                
            
        #     logging.info(f"Successfully loaded {table} into Postgres")

        # except Exception as e:
        #     logging.error(f"Failed loading {table}: {str(e)}")


def load_to_postgres(spark):
    try:
        logging.info(f"Starting load for {table}")
        df = spark.read.parquet(f"{file_path}/{table}")

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", dbuser) \
            .option("password", dbpassword) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Successfully loaded {table} into Postgres")
    except Exception as e:
        logging.error(f"Failed loading {table}: {str(e)}")



