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
from jobs.transactions.build_all_transactions import flip_sign
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import matplotlib.pyplot as plt
import calendar
from shared.utilities import read_latest_file_from_hdfs


def get_aggregations(year, month, in_category, all=False, flipsign=True):
    """
    This function builds the aggregations as needed for that year and month.
    It also produces histograms in a specific category.

    :param year: input year
    :param month: input month
    :param in_category: input category for histogram
    :param all: for histogram only past 10 months
    :param flipsign: flipsign is used for reversing the bars. required for only fewer categories like pay.
    :return: returns aggregations and histograms
    """

    # getting the main master file path
    all_transactions_path = config["target"]["all_master"]
    # reading the category flags
    category_flags_path = config["lookup"]["category_flags"]
    # reading all the descriptions
    desc_flags_path = config["lookup"]["description_flags"]

    # extracting date values as needed
    start_date = str(year)+month+'01'
    _, end_day = calendar.monthrange(year, int(month))
    end_date = str(year)+month+str(end_day)

    # reading in the latest all master file
    latest_file = read_latest_file_from_hdfs(spark, all_transactions_path, match_filename='20')
    all_transactions = spark.read.csv(latest_file, sep=',', header=True)

    # if all = true then it is for histogram or it is only monthly expenditure

    if not all:
        # filtering that respective months transations
            monthly_transactions = all_transactions.filter(f"trndt between {start_date} and {end_date}")
            category_flags = spark.read.csv(category_flags_path, sep=',', header=True)
            desc_flags = spark.read.csv(desc_flags_path, sep=',', header=True)

            desc_only_flags = desc_flags.select('DESCRIPTIONS')

            Flag_df = [str(i.DESCRIPTIONS) for i in desc_only_flags.collect()]

            # converting it to pandas dataframe
            pandas_monthly_transactions = monthly_transactions.toPandas()

            # rewwrite the new descriptions to the transactions
            for i in Flag_df:
                pandas_monthly_transactions.loc[pandas_monthly_transactions['Description'].str.contains(i, case=False), 'new_Description'] = i

            headers = ["Transaction_date", "Description", "Amount", "trndt", "act_type", "new_Description"]
            schema = StructType([StructField(col, StringType()) for col in headers])

            # converting back to spark dataframe
            transactions = spark.createDataFrame(pandas_monthly_transactions, schema=schema)

            t = transactions.alias('t')
            f = desc_flags.alias('f')

            # flags the transactions
            transactions = t.join(f, t.new_Description == f.DESCRIPTIONS, "left_outer").drop("DESCRIPTIONS")

            print('No of null amounts are', transactions.filter("Amount is null").count())

            transactions = transactions.filter("Amount is not null")

            # replaces commas in the amount field
            transactions = transactions.withColumn("Amount", replace_comma("Amount"))

            transactions.show(20, False)

            # gives the number of transactions per account type in a month
            print('number of transactions per account type in a month')
            transactions.groupby('act_type').agg(F.count("act_type").alias("total_act_type_transactions")).show()

            # gives the latest transaction date per each account
            print('latest transaction date per each account')
            transactions.groupby('act_type').agg(F.max("trndt").alias("max_trndt")).sort("max_trndt").show()

            # gives the new transaction list
            print('brand new transactions')
            null_flags = transactions.filter("FLAG is null")
            null_flags.orderBy(F.asc("trndt")).show(100, False)

            print(f'checking for transactions under category {in_category}')
            filter_flags = transactions.filter(f"FLAG='{in_category}'")
            filter_flags.orderBy(F.asc("trndt")).show(100, False)

            grouped_df = transactions.groupby('FLAG').agg(F.sum("Amount").alias("total_amt"))
            print('grouped_df per category')
            grouped_df = grouped_df.join(category_flags, "FLAG", "left_outer")\
                         .orderBy(F.desc("total_amt"))

            grouped_df = grouped_df.filter("FLAG<>'PAY'")
            grouped_df.show(200, False)

            y = grouped_df.select("total_amt").rdd.map(lambda row: row[0]).collect()

            print(y)
            print('incoming', sum(i for i in y if i > 0))
            print('outgoing', sum(i for i in y if i < 0))

            # all_transactions = all_transactions.withColumn("year_month", F.substring(F.col("trndt"), 1, 6))
            # all_transactions.groupBy("year_month","")
            # all_transactions.show(200, False)
    else:
        category_flags = spark.read.csv(category_flags_path, sep=',', header=True)
        desc_flags = spark.read.csv(desc_flags_path, sep=',', header=True)

        category_flags.show(100, False)

        desc_only_flags = desc_flags.select('DESCRIPTIONS')
        Flag_df = [str(i.DESCRIPTIONS) for i in desc_only_flags.collect()]

        pandas_monthly_transactions = all_transactions.toPandas()

        for i in Flag_df:
            pandas_monthly_transactions.loc[
                pandas_monthly_transactions['Description'].str.contains(i, case=False), 'new_Description'] = i

        headers = ["Transaction_date", "Description", "Amount", "trndt", "act_type", "new_Description"]
        schema = StructType([StructField(col, StringType()) for col in headers])

        transactions = spark.createDataFrame(pandas_monthly_transactions, schema=schema)

        t = transactions.alias('t')
        f = desc_flags.alias('f')

        transactions = t.join(f, t.new_Description == f.DESCRIPTIONS, "left_outer").drop("DESCRIPTIONS")

        print('No of null amounts are', transactions.filter("Amount is null").count())

        transactions = transactions.filter("Amount is not null")

        transactions = transactions.withColumn("Amount", replace_comma("Amount"))

        transactions.show(20, False)

        transactions.groupby('act_type').agg(F.count("act_type").alias("total_act_type_transactions")).show()

        transactions.groupby('act_type').agg(F.max("trndt").alias("max_trndt")).sort("max_trndt").show()

        transactions = transactions.withColumn("trn_month", F.expr("concat(substr(trndt, 3, 2),'-',substr(trndt, 5, 2))"))



                                              # F.concat(F.substring(F.col("trndt"), 3, 4), "-", F.substring(F.col("trndt"), 4, 5)))

        null_flags = transactions.filter("FLAG is null")
        null_flags.orderBy(F.asc("trndt")).show(100, False)

        filter_flags = transactions.filter(f"FLAG='{in_category}'")
        # filter_flags = transactions.filter("act_type='bofacredit'")
        filter_flags.orderBy(F.asc("trndt")).show(100, False)

        # filter_flags_2 = transactions.filter("FLAG='GR'")
        # # filter_flags = transactions.filter("act_type='bofacredit'")
        # filter_flags_2.orderBy(F.asc("trndt")).show(100, False)



        grouped_df = transactions.groupby('FLAG','trn_month').agg(F.sum("Amount").alias("total_amt"))
        print('grouped_df')
        grouped_df = grouped_df.join(category_flags, "FLAG", "left_outer") \
            .orderBy(F.desc("trn_month"))

        grouped_df.show(200, False)

        grouped_df = grouped_df.filter(f"FLAG='{in_category}'").orderBy(F.asc("trn_month"))

        grouped_df = grouped_df.filter("FLAG<>'PAY'")

        print_category = grouped_df.select("CATEGORY").filter(f"FLAG='{in_category}'").distinct().rdd.map(lambda row : row[0]).collect()

        grouped_df.orderBy(F.desc("trn_month")).show()

        if flipsign:
            grouped_df = grouped_df.withColumn("total_amt", flip_sign("total_amt"))

        # plotting the histograms
        x = grouped_df.select("trn_month").rdd.map(lambda row : row[0]).collect()
        y = grouped_df.select("total_amt").rdd.map(lambda row : row[0]).collect()

        x = x[-10:]
        y = y[-10:]

        avg = Average(y)

        print(x)
        print(y)
        print(avg)

        plt.bar(x, y, align='center')
        plt.ylabel(f'{print_category}')
        plt.xlabel('[months]')
        plt.title(f'avg in 10 months is {avg}')
        for i in range(len(y)):
            plt.hlines(y[i], 0, x[i])  # Here you are drawing the horizontal lines
        plt.show()

        # all_transactions = all_transactions.withColumn("year_month", F.substring(F.col("trndt"), 1, 6))
        # all_transactions.groupBy("year_month","")
        # all_transactions.show(200, False)




    pass


replace_comma = F.udf(lambda s: s.replace(",", ""))

def Average(lst):
    """

    :param lst:
    :return: returns the average value
    """
    return sum(lst) / len(lst)


#get_aggregations(year=2019, month='02')
get_aggregations(year=2019, month='02', in_category='LIQ', all=True, flipsign=True)


