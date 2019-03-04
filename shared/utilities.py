import PyPDF2
from tabula import read_pdf
from pyspark.sql.types import StructField, StructType, StringType
import os
import camelot
from pyspark.sql import functions as F
import datetime



def scd1(incoming_df, master_df):

    m = master_df
    i = incoming_df

    headers = ["Transaction_date", "Description", "Reference_number"]

    delisted_df = m.alias('m').join(i.alias('i'), headers, "left_outer").filter("i.Transaction_date is NULL").select('m.*')
    print("dfs")
    m.show()
    i.show()
    delisted_df.show()

    FINAL_DF = delisted_df.union(incoming_df)
    return FINAL_DF


def write_bofa_cc_to_master(spark, src_path, cycle_date):
    """

    :param src_path:
    :return: reads a pdf file and returns a dataframe
    """

    cycle_year = cycle_date[:4]

    print(src_path)

    reader = PyPDF2.PdfFileReader(open(src_path, mode='rb'))
    n = reader.getNumPages()  # to get the count of  number of pages in a PDF
    print('no of pages are ' + str(n))
    header = ["Transaction_date", "Posting_date", "Description", "Reference_number", "Account_number", "Amount",
              "Total"]
    schema = StructType([StructField(col, StringType()) for col in header])
    union_df = spark.createDataFrame([], schema)

    for i in range(3, n + 1):

        print('page no in processing ' + str(i))

        try:
            df1 = read_pdf(src_path, pages=i)
            numof_coloumns = len(df1.columns)
            print(str(numof_coloumns) + '  numof_coloumns')

            try:

                # df2 =read_pdf(filename,pages=4)

                if numof_coloumns == 7:

                    headers = ["Transaction_date", "Posting_date", "Description", "Reference_number", "Account_number",
                               "Amount", "Total"]
                    schema = StructType([StructField(col, StringType()) for col in headers])
                elif numof_coloumns == 8:

                    headers = ["Transaction_date", "Posting_date", "Description", "other", "Reference_number",
                               "Account_number", "Amount", "Total"]
                    schema = StructType([StructField(col, StringType()) for col in headers])

                df1 = spark.createDataFrame(df1, schema=schema)

                if numof_coloumns == 8:
                    df1 = df1.drop("other")
                    # .where("Transaction_date like '%%/%%'")

                union_df = df1.select(*header).union(union_df.select(*header))

                # df1.show(50)
            except ValueError:
                pass


        except AttributeError:
            pass

    union_df = union_df.where("Transaction_date like '%%/%%'")
    union_df = union_df.withColumn("Transaction_date", F.concat(F.col('Transaction_date'), F.lit('/' + cycle_year)))


    return union_df


def write_src_csv_to_master(src_df, master, account_type):
    """

    :param src_path:
    :return: reads a pdf file and returns a dataframe
    """
    src_df = src_df.withColumn('act_type', F.lit(account_type))
    src_df.coalesce(1).write.format("csv").mode("overwrite").save(master, header="true")
    return src_df


def convert_date_format(df, col, in_format, out_format):
    """

    :param col:
    :return:
    """

    df = df.withColumn('trndt', F.udf(lambda d: datetime.datetime.strptime(d, in_format).strftime(out_format), StringType())(F.col(col)))

    return df


