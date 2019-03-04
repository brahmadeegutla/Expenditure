from shared.utilities import write_bofa_cc_to_master, write_src_csv_to_master, convert_date_format
import calendar
import datetime
import pyspark.sql.functions as F

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
        cycle_month = '{}'.format(cycle_date[4:6])
        cycle_month = calendar.month_name[int(cycle_month)]

        src_path = config["source"]["citi"] + 'tabula-' + cycle_year +' '+ cycle_month + '*.csv'
        master_path = config["target"]["citi_master"] + cycle_date

        src_df = spark.read.csv(src_path, header=True, sep=',')

        src_df = convert_date_format(src_df, 'transdate', '%d-%b', '%m%d')

        src_df = src_df.withColumn("trndt", F.concat(F.lit(cycle_year), 'trndt'))

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

        cycle_date_input = '{}-{}'.format(cycle_date[0:4], cycle_date[4:6])
        trg_cycle_date = '{}{}'.format(cycle_date[0:4], cycle_date[4:6])

        src = config["source"]["bofa_credit"] + 'eStmt_' + cycle_date_input + '-15.pdf'
        master = config["target"]["bofa_cc_master"] + trg_cycle_date

        new_df = write_bofa_cc_to_master(spark, src, cycle_date)

        new_df = convert_date_format(new_df, 'Transaction_date', '%m/%d/%Y', '%Y%m%d')

        new_df = new_df.withColumn('act_type', F.lit(account_type))

        new_df.coalesce(1).write.format("csv").mode("overwrite").save(master, header="true")

    else:
        print('Please put in the right account_type: from citi discover bofachk bofacredit')
        exit()


    new_df.show(200, False)

    pass