from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml
import utils.apps_utils as ut


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )


    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']
    staging_dir = app_conf["s3_conf"]["staging_dir"]
    for src in src_list:
        src_conf = app_conf[src]
        stg_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + staging_dir + "/" + src
        if src == 'SB':
            print("\nReading data from MySQL DB - SB,")
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                           "lowerBound": "1",
                           "upperBound": "100",
                           "dbtable": src_conf["mysql_conf"]["query"],
                           "numPartitions": "2",
                           "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                           "user": app_secret["mysql_conf"]["username"],
                           "password": app_secret["mysql_conf"]["password"]
                           }
            sb_df = ut.read_from_mysql(jdbc_params, spark)\
                .withColumn("run_dt", current_date())
            sb_df.show()

            sb_df.write\
                .partitionBy("run_dt")\
                .mode("overwrite")\
                .parquet(stg_path)
        elif src == 'OL':
            sftp_options = {
                "host": app_secret["sftp_conf"]["hostname"],
                "port": app_secret["sftp_conf"]["port"],
                "username": app_secret["sftp_conf"]["username"],
                "pem": os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]),
                "fileType": "csv",
                "delimiter": "|"
            }
            file_loc = src_conf["sftp_conf"]["directory"] + "/" + src_conf["sftp_conf"]["filename"]
            ol_df = ut.read_from_sftp(spark, file_loc, sftp_options)\
                .withColumn("run_dt", current_date())

            ol_df.show()

            ol_df.write\
                .partitionBy("run_dt")\
                .mode("overwrite")\
                .parquet(stg_path)

        elif src == 'CP':
            cp_df = spark.read \
                .option("header", "true") \
                .option("delimiter", "|") \
                .format("csv") \
                .option("inferSchema", "true") \
                .load("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/" + src_conf["s3_conf"]["cust_dir"] + "/" + src_conf["s3_conf"]["filename"]) \
                .withColumn("run_dt", current_date())

            cp_df.show()

            cp_df \
                .write \
                .partitionBy("run_dt") \
                .mode("overwrite") \
                .parquet(stg_path)

        elif src == "ADDR":
            addr_df = spark \
                .read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", src_conf["mongodb_config"]["database"]) \
                .option("collection", src_conf["mongodb_config"]["collection"]) \
                .load()

            addr_df.show()

            addr_df = addr_df.select(col('consumer_id'),
                           col("address.street").alias("street"),
                           col("address.city").alias("city"),
                           col("address.state").alias("state"),
                           col("mobile-no"),
                           col("run_dt")) \
                .withColumn("run_dt", current_date())

            addr_df \
                .write \
                .partitionBy("run_dt") \
                .mode("overwrite") \
                .parquet(stg_path)





# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/uniliver/source_data_loading.py
# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/others/systems/sftp_df.py
#spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/uniliver/source_data_loading.py
#spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/uniliver/source_data_loading.py