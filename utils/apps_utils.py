from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def read_from_mysql(jdbc_params, spark):
    return spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()


def read_from_sftp(spark,file_loc,sftp_options):
    return spark \
        .read.format("com.springml.spark.sftp") \
        .options(**sftp_options)\
        .load(file_loc)

def write_to_redshift_reg(spark,jdbc_url,app_conf):
    spark.coalesce(1).write \
           .format("io.github.spark_redshift_community.spark.redshift") \
           .option("url", jdbc_url) \
           .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
           .option("forward_spark_s3_credentials", "true") \
           .option("dbtable", "DATAMART.REGIS_DIM") \
           .mode("overwrite") \
           .save()

def write_to_redshift_child(spark,jdbc_url,app_conf):
    spark.coalesce(1).write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", "DATAMART.CHILD_DIM") \
        .mode("overwrite") \
        .save()

def read_to_redshift(spark, jdbc_url, app_conf):
    spark.read\
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("query", app_conf["redshift_conf"]["query"]) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .load()

def write_to_redshift(spark,jdbc_url,app_conf):
    spark.coalesce(1).write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", "DATAMART.RTL_TXN_FCT") \
        .mode("overwrite") \
        .save()


def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)