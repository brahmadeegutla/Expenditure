import os
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = 'python3.7'
spark = SparkSession.builder.appName("tosparkdf").getOrCreate()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
from resources.config import app_config as config
import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as t


def run_all_transactions(spark, config):
    """

    :param spark:
    :param config:
    :param cycle_date:
    :param account_type:
    :return:
    """
    today_date = datetime.datetime.today().strftime('%Y%m%d')

    all_master_path = os.path.join(config['target']['all_master'], today_date)

    bofa_cc_master = config['target']['bofa_cc_master']+ '*/*.csv'
    bofa_chk_master = config['target']['bofa_chk_master']+ '*/*.csv'
    discover_master = config['target']['discover_master']+ '*/*.csv'
    citi_master = config['target']['citi_master']+ '*/*.csv'

    bofacredit = ['Transaction_date', 'Description', 'Amount', 'trndt', 'act_type']
    bofachk = ['Date', 'Description', 'Amount', 'trndt', 'act_type']
    discover = ['Trans_Date', 'Description', 'Amount', 'trndt', 'act_type']
    citi = ['transdate', 'Description', 'Amount', 'trndt', 'act_type']

    master_header = ['Transaction_date', 'Description', 'Amount', 'trndt', 'act_type']




    bofa_chk_master = spark.read.csv(bofa_chk_master, header=True, sep=',').select(*bofachk)
    discover_master = spark.read.csv(discover_master, header=True, sep=',').select(*discover)
    citi_master = spark.read.csv(citi_master, header=True, sep=',').select(*citi)
    bofa_cc_master_df = spark.read.csv(bofa_cc_master, header=True, sep=',').select(*bofacredit)

    citi_master = citi_master.\
        withColumn('Amount', replace_lp("Amount")).\
        withColumn('Amount', replace_rp("Amount")).\
        withColumn('Amount', replace_dol("Amount"))

    discover_master = discover_master.withColumn("Amount", flip_sign("Amount"))
    bofa_cc_master_df = bofa_cc_master_df.withColumn("Amount", flip_sign("Amount"))
    citi_master = citi_master.withColumn("Amount", flip_sign("Amount"))


    master_df = bofa_cc_master_df.\
        union(bofa_chk_master).\
        union(discover_master).\
        union(citi_master).toDF(*master_header)

    master_df = master_df.withColumn('Description', F.upper(F.col('Description')))

    master_df.filter("Description like '%PAYMENT%'").show(100, False)


    master_df.coalesce(1).write.format("csv").mode("overwrite").save(all_master_path, header="true")

    pass


def flip_sign(column):
    return F.when(F.col(column).isNotNull(), -F.col(column)).otherwise(0)


replace_lp = F.udf(lambda s: s.replace("(", "-"))
replace_rp = F.udf(lambda s: s.replace(")", ""))
replace_dol = F.udf(lambda s: s.replace("$", ""))


run_all_transactions(spark, config)

