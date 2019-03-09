import os
from pyspark.sql import SparkSession
from resources.config import app_config as config
from pyspark.sql.types import StringType, StructField, StructType
os.environ['PYSPARK_PYTHON'] = 'python3.7'
spark = SparkSession.builder.appName("tosparkdf").getOrCreate()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
import logging
import pandas
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import calendar

def get_aggregations(year, month):
    """

    :param cycle_date:
    :return:
    """
    all_transactions_path = config["target"]["all_master"]
    category_flags_path = config["lookup"]["category_flags"]
    desc_flags_path = config["lookup"]["description_flags"]

    start_date = str(year)+month+'01'
    _, end_day = calendar.monthrange(year, int(month))
    end_date = str(year)+month+str(end_day)

    all_transactions = spark.read.csv(all_transactions_path+'/*/*.csv', sep=',', header=True)
    monthly_transactions = all_transactions.filter(f"trndt between {start_date} and {end_date}")
    category_flags = spark.read.csv(category_flags_path, sep=',', header=True)
    desc_flags = spark.read.csv(desc_flags_path, sep=',', header=True)

    monthly_transactions.show()

    desc_only_flags = desc_flags.select('DESCRIPTIONS')
    Flag_df = [str(i.DESCRIPTIONS) for i in desc_only_flags.collect()]
    print(Flag_df)

    pandas_monthly_transactions = monthly_transactions.toPandas()

    for i in Flag_df:
        pandas_monthly_transactions.loc[pandas_monthly_transactions['Description'].str.contains(i, case=False), 'new_Description'] = i

    headers = ["Transaction_date", "Description", "Amount", "trndt", "act_type", "new_Description"]
    schema = StructType([StructField(col, StringType()) for col in headers])

    transactions = spark.createDataFrame(pandas_monthly_transactions, schema=schema)

    t = transactions.alias('t')
    f = desc_flags.alias('f')

    transactions = t.join(f, t.new_Description == f.DESCRIPTIONS, "left_outer").drop("DESCRIPTIONS")

    transactions = transactions.withColumn("Amount", replace_comma("Amount"))

    transactions.show(20, False)

    transactions.groupby('act_type').agg(F.count("act_type").alias("total_act_type_transactions")).show()

    null_flags = transactions.filter("FLAG = 'FD'")
    null_flags.orderBy(F.asc("trndt")).show(100, False)

    grouped_df = transactions.groupby('FLAG').agg(F.sum("Amount").alias("total_amt"))
    print('grouped_df')
    grouped_df.join(category_flags, "FLAG", "left_outer")\
        .orderBy(F.desc("total_amt")).show(200, False)

    pass


replace_comma = F.udf(lambda s: s.replace(",", ""))

get_aggregations(year=2018, month='09')


