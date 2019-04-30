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
    cycle_month = cycle_date[4:6]
    previous_year = int(cycle_year) - 1

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

    if cycle_month == '01':
        union_df = union_df.withColumn("Transaction_date", F.expr(f"case when Transaction_date like '12%' then concat(Transaction_date,'/',{previous_year})"
                                                      f" else concat(Transaction_date,'/',{cycle_year}) end"))
    else:
        union_df = union_df.withColumn("Transaction_date", F.expr(f"concat(Transaction_date,'/',{cycle_year})"))


    return union_df


def write_chase_chk_pdf_to_master(spark, src_path, cycle_date):
    """

    :param src_path:
    :return: reads a pdf file and returns a dataframe
    """

    cycle_year = cycle_date[:4]
    cycle_month = cycle_date[4:6]
    previous_year = int(cycle_year)-1

    src_file = get_file_starting_with(spark,src_path, match_filename=cycle_date)
    src_path = os.path.join(src_path, src_file)

    reader = PyPDF2.PdfFileReader(open(src_path, mode='rb'))
    n = reader.getNumPages()  # to get the count of  number of pages in a PDF
    print('no of pages are ' + str(n))
    header = ["date", "description", "amount", "balance"]
    schema = StructType([StructField(col, StringType()) for col in header])
    union_df = spark.createDataFrame([], schema)

    for i in range(1, n+1):

        print('page no in processing ' + str(i))

        try:
            df1 = read_pdf(src_path, pages=i)
            print(df1)
            numof_coloumns = len(df1.columns)
            print(str(numof_coloumns) + '  numof_coloumns')

            try:

                headers = ["date", "description", "amount", "balance"]
                schema = StructType([StructField(col, StringType()) for col in headers])

                df1 = spark.createDataFrame(df1, schema=schema)



                df1 = df1.filter("amount not like '%NaN%'")

                union_df = df1.select(*header).union(union_df.select(*header))

            except ValueError:
                pass


        except AttributeError:
            pass

    union_df = union_df.filter("amount not like '%NaN%'")
    union_df = union_df.withColumn("description", F.expr("substring(date, 7, length(date))"))
    union_df = union_df.withColumn("date", F.substring(F.col("date"), 1, 5))

    if cycle_month == '01':
        union_df = union_df.withColumn("date", F.expr(f"case when date like '12%' then concat(date,'/',{previous_year})"
                                                      f" else concat(date,'/',{cycle_year}) end"))
    else:
        union_df = union_df.withColumn("date", F.expr(f"concat(date,'/',{cycle_year})"))

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


def read_latest_file_from_hdfs(spark, file_path, match_filename=None):
    """
    This function accepts path of the parent location of a file, and matching condition.
    And returns the latest max value.
    :param spark: pyspark session
    :param fs: filesystem access
    :param file_path: Path of the file till parent level
    :param match_filename: match name of the childs folders
    :return:
    """
    filename = []
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
    for fs_obj in fs.listStatus(hdfs_path):
        filename.append(fs_obj.getPath().getName())
    filename = [s for s in filename if match_filename.lower() in s.lower()]

    print(filename)
    filename = filter(lambda x: x.startswith(match_filename), filename)
    print(filename)
    words = [w.replace(match_filename, '') for w in filename]
    words = [int(x) if x.isdigit() else x for x in words]
    words = [x for x in words if isinstance(x, int)]
    latest_file_date = max(words)
    latest_file = match_filename + str(latest_file_date)
    latest_full_path = os.path.join(file_path, latest_file)
    return latest_full_path


def get_file_starting_with(spark, file_path, match_filename=None):
    filename = []
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
    for fs_obj in fs.listStatus(hdfs_path):
        filename.append(fs_obj.getPath().getName())
    filename = [s for s in filename if match_filename.lower() in s.lower()]

    filename = ''.join(filename)
    return filename




