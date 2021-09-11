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

    tgt_list = app_conf['target_list']
    staging_dir = app_conf["s3_conf"]["staging_dir"]
    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]
        stg_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + staging_dir + "/"
        if tgt == 'REGIS_DIM':

            cp_df = spark.read \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/CP")
                #.filter(col("run_dt") = current_date())

            cp_df.show()
            cp_df.createOrReplaceTempView("CP")
            regis_dim_df = spark.sql(tgt_conf['loadingQuery'])
            regis_dim_df.show()
            print("Writing regis_txn_fact dataframe to AWS Redshift Table   >>>>>>>")

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            # create function for this

            regis_dim_df = ut.write_to_redshift_reg(spark,jdbc_url,app_conf)



        elif tgt == 'CHILD_DIM':
            cp_df = spark.read \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/CP")
            # .filter(col("run_dt") = current_date())

            cp_df.show()
            cp_df.createOrReplaceTempView("CP")
            child_dim_df = spark.sql(tgt_conf['loadingQuery'])
            child_dim_df.show()

            print("Writing child_txn_fact dataframe to AWS Redshift Table   >>>>>>>")

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            # create a function for this
            child_dim_df = ut.write_to_redshift_child(spark,jdbc_url,app_conf)

        elif tgt == 'RTL_TXN_FCT':
            # iterate over the list of parquet

            src_list_test = ["SB","OL"]
            for src in src_list_test():
                test = src+"_df"
                test = spark.read.parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + src)




                # create a function for this
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            reg_df = ut.read_to_S3(spark,jdbc_url,app_conf)

            # .filter(col("run_dt") = current_date())

            test.show()
            #ol_df.show()
            reg_df.show(5, False)

            test.createOrReplaceTempView("SB")
           # ol_df.createOrReplaceTempView("OL")
            reg_df.createOrReplaceTempView("REGIS_DIM")
            rtl_fct_df = spark.sql(tgt_conf['loadingQuery'])
            rtl_fct_df.show()




            print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            rtl_fct_df.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.RTL_TXN_FCT") \
                .mode("overwrite") \
                .save()

    print("Completed   <<<<<<<<<")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/uniliver/target_data_loading.py
# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/uniliver/target_data_loading.py


