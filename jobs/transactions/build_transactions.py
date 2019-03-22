from shared.utilities import write_bofa_cc_to_master, write_src_csv_to_master, convert_date_format, \
    write_chase_chk_pdf_to_master, get_file_starting_with
import calendar
from pyspark.sql.types import StructField, StructType, StringType
import datetime
import pyspark.sql.functions as F
import os

def run_transactions(spark, config, cycle_date, account_type):
    """

    :param spark:
    :param config:
    :param cycle_date:
    :param account_type:
    :return:
    """
    if account_type == 'citi':

        cycle_year = '{}'.format(cycle_date[0:4])
        cycle_month_num = '{}'.format(cycle_date[4:6])
        cycle_month = calendar.month_name[int(cycle_month_num)]
        previous_year = int(cycle_year) - 1

        src_path = config["source"]["citi"] + 'tabula-' + cycle_year +' '+ cycle_month + '*.csv'
        master_path = config["target"]["citi_master"] + cycle_date

        headers = ["transdate", "Posting_date", "Description", "Amount"]

        src_df = spark.read.csv(src_path, sep=',')

        new_df = src_df.toDF(*headers).filter('Amount is not null')

        if cycle_month_num == '01':
            new_df = new_df.withColumn("transdate",
                                           F.expr(f"case when transdate like '%Dec%' then concat(transdate,'-',{previous_year})"
                                                  f" else concat(transdate,'-',{cycle_year}) end"))
        else:
            new_df = new_df.withColumn("transdate", F.expr(f"concat(transdate,'-',{cycle_year})"))

        src_df = convert_date_format(new_df, 'transdate', '%d-%b-%Y', '%Y%m%d')

        new_df = write_src_csv_to_master(src_df, master_path, account_type)

    elif account_type == 'discover':

        src_path = config["source"]["discover"] + 'Discover-Statement-' + cycle_date + '*.csv'
        master_path = config["target"]["discover_master"] + cycle_date

        src_df = spark.read.csv(src_path, header=True, sep=',')

        columns = [c.replace('. ', '_') for c in src_df.columns]

        src_df = src_df.toDF(*columns)

        src_df = convert_date_format(src_df, 'Trans_Date', '%m/%d/%Y', '%Y%m%d')

        new_df = write_src_csv_to_master(src_df, master_path, account_type)

    elif account_type == 'bofachk':

        cycle_date_input = '{}-{}'.format(cycle_date[0:4], cycle_date[4:6])
        trg_cycle_date = '{}{}'.format(cycle_date[0:4], cycle_date[4:6])

        src_path = config["source"]["bofa_checking"] + 'tabula-eStmt_' + cycle_date_input + '*.csv'
        master_path = config["target"]["bofa_chk_master"] + trg_cycle_date

        src_df = spark.read.csv(src_path, header=True, sep=',').filter('Amount is not null')

        src_df = convert_date_format(src_df, 'Date', '%m/%d/%y', '%Y%m%d')

        new_df = write_src_csv_to_master(src_df, master_path, account_type)

    elif account_type == 'bofacredit':

        cycle_year = '{}'.format(cycle_date[0:4])
        cycle_month = '{}'.format(cycle_date[4:6])
        previous_year = int(cycle_year) - 1

        # cycle_date_input = '{}-{}'.format(cycle_date[0:4], cycle_date[4:6])
        # trg_cycle_date = '{}{}'.format(cycle_date[0:4], cycle_date[4:6])
        #
        # src = config["source"]["bofa_credit"] + 'eStmt_' + cycle_date_input + '-15.pdf'
        # master = config["target"]["bofa_cc_master"] + trg_cycle_date
        #
        #new_df = write_bofa_cc_to_master(spark, src, cycle_date)
        #
        # new_df = convert_date_format(new_df, 'Transaction_date', '%m/%d/%Y', '%Y%m%d')
        #
        # new_df = new_df.withColumn('act_type', F.lit(account_type))
        #
        # new_df.coalesce(1).write.format("csv").mode("overwrite").save(master, header="true")

        headers = ["Transaction_date", "Posting_date", "Description",  "Amount"]

        cycle_date_input = '{}-{}'.format(cycle_date[0:4], cycle_date[4:6])
        trg_cycle_date = '{}{}'.format(cycle_date[0:4], cycle_date[4:6])

        src_path = config["source"]["bofa_credit"] + 'tabula-eStmt_' + cycle_date_input + '*.csv'
        master_path = config["target"]["bofa_cc_master"] + trg_cycle_date

        src_df = spark.read.csv(src_path, header=False, sep=',')
        src_df = src_df.toDF(*headers)

        if cycle_month == '01':
            src_df = src_df.withColumn("Transaction_date",
                                           F.expr(f"case when Transaction_date like '%Dec%' then concat(Transaction_date,'-',{previous_year})"
                                                  f" else concat(Transaction_date,'-',{cycle_year}) end"))
        else:
            src_df = src_df.withColumn("Transaction_date", F.expr(f"concat(Transaction_date,'-',{cycle_year})"))

        src_df = src_df.filter('AMOUNT is not null')

        src_df = convert_date_format(src_df, 'Transaction_date', '%d-%b-%Y', '%Y%m%d')

        new_df = write_src_csv_to_master(src_df, master_path, account_type)

    elif account_type == 'chase':

        cycle_date_input = '{}-{}'.format(cycle_date[0:4], cycle_date[4:6])
        cycle_year = cycle_date[0:4]
        cycle_month = cycle_date[4:6]
        previous_year = int(cycle_year) - 1
        trg_cycle_date = '{}{}'.format(cycle_date[0:4], cycle_date[4:6])

        src_path = config["source"]["chase"]
        master = config["target"]["chase_master"] + trg_cycle_date
        headers = ["date", "description", "amount", "balance"]

        src_file_name = get_file_starting_with(spark, src_path, match_filename=cycle_date)
        src_path = os.path.join(src_path, src_file_name)

        src_df = spark.read.csv(src_path, header=True, sep=',').filter('AMOUNT is not null')

        new_df = src_df.toDF(*headers)


        #new_df = write_chase_chk_pdf_to_master(spark, src, cycle_date)

        if cycle_month == '01':
            new_df = new_df.withColumn("date",
                                           F.expr(f"case when date like '%Dec%' then concat(date,'-',{previous_year})"
                                                  f" else concat(date,'-',{cycle_year}) end"))
        else:
            new_df = new_df.withColumn("date", F.expr(f"concat(date,'-',{cycle_year})"))

        new_df = convert_date_format(new_df, 'DATE', '%d-%b-%Y', '%Y%m%d')

        new_df.show()

        new_df = new_df.withColumn('act_type', F.lit(account_type))

        new_df.coalesce(1).write.format("csv").mode("overwrite").save(master, header="true")

    else:
        print('Please put in the right account_type: from citi discover bofachk bofacredit chase')

    new_df.show(200, False)

    pass