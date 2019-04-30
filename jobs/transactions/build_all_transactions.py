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
    This funtion reads all the master files from each account type and writes it to one main master file
    which is being used for further aggregations
    :param spark:
    :param config:
    :return:
    """
    # today's date to have it for the filename
    today_date = datetime.datetime.today().strftime('%Y%m%d')

    # all master path
    all_master_path = os.path.join(config['target']['all_master'], today_date)

    # master paths for individual masters
    bofa_cc_master = config['target']['bofa_cc_master']+ '*/*.csv'
    bofa_chk_master = config['target']['bofa_chk_master']+ '*/*.csv'
    discover_master = config['target']['discover_master']+ '*/*.csv'
    citi_master = config['target']['citi_master']+ '*/*.csv'
    chase_master = config['target']['chase_master'] + '*/*.csv'

    # headers for each master file
    bofacredit = ['Transaction_date', 'Description', 'Amount', 'trndt', 'act_type']
    bofachk = ['Date', 'Description', 'Amount', 'trndt', 'act_type']
    discover = ['Trans_Date', 'Description', 'Amount', 'trndt', 'act_type']
    citi = ['transdate', 'Description', 'Amount', 'trndt', 'act_type']
    chase = ['date', 'description', 'amount', 'trndt', 'act_type']

    # all master header
    master_header = ['Transaction_date', 'Description', 'Amount', 'trndt', 'act_type']

    # reading master files
    bofa_chk_master = spark.read.csv(bofa_chk_master, header=True, sep=',').select(*bofachk)
    discover_master = spark.read.csv(discover_master, header=True, sep=',').select(*discover)
    citi_master = spark.read.csv(citi_master, header=True, sep=',').select(*citi)
    bofa_cc_master_df = spark.read.csv(bofa_cc_master, header=True, sep=',').select(*bofacredit)
    chase_master_df = spark.read.csv(chase_master, header=True, sep=',').select(*chase)

    # tweaking citi amount since they have special characters like ( ) and dollar signs
    citi_master = citi_master.\
        withColumn('Amount', replace_lp("Amount")).\
        withColumn('Amount', replace_rp("Amount")).\
        withColumn('Amount', replace_dol("Amount"))

    # flipping the credit card amounts to match buying vs spending
    discover_master = discover_master.withColumn("Amount", flip_sign("Amount"))
    bofa_cc_master_df = bofa_cc_master_df.withColumn("Amount", flip_sign("Amount"))
    citi_master = citi_master.withColumn("Amount", flip_sign("Amount"))

    # clubbing all the transactions together
    master_df = bofa_cc_master_df.\
        union(bofa_chk_master).\
        union(discover_master).\
        union(citi_master).\
        union(chase_master_df).toDF(*master_header)

    master_df = master_df.withColumn('Description', F.upper(F.col('Description')))

    master_df.show(500, False)

    # writing it to main master
    master_df.coalesce(1).write.format("csv").mode("overwrite").save(all_master_path, header="true")

    pass


def flip_sign(column):
    """
    This functions is used to flip the sign
    :param column:
    :return: return neg to positive value or vise versa
    """
    return F.when(F.col(column).isNotNull(), -F.col(column)).otherwise(0)


# user defined functions to replace special characters

replace_lp = F.udf(lambda s: s.replace("(", "-"))
replace_rp = F.udf(lambda s: s.replace(")", ""))
replace_dol = F.udf(lambda s: s.replace("$", ""))


run_all_transactions(spark, config)

