import os
from pyspark.sql import SparkSession
from resources.config import app_config as config
from jobs.transactions.build_transactions import  run_transactions
os.environ['PYSPARK_PYTHON'] = 'python3.7'
spark = SparkSession.builder.appName("tosparkdf").getOrCreate()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
import logging


def main(cycle_date, account_type):

    print(account_type + ' running for cycle_date ' + cycle_date)

    try:
        _ = run_transactions(spark, config, cycle_date, account_type)

    except Exception as ex:
        logging.error(str(ex))
        raise Exception(str(ex))
    finally:
        spark.stop()


main(cycle_date='201901', account_type='citi')


