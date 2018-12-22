import PyPDF2
import pandas as pd
from tabula import wrapper, read_pdf
from tabulate import tabulate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType
from pyspark.sql import functions as F
import  os
from utilities import scd1
os.environ['PYSPARK_PYTHON']='python3.7'
#os.environ['PYSPARK_DRIVER_PYTHON']='python2.7'
spark = SparkSession.builder.appName("tosparkdf").getOrCreate()


#def create_bofa_cc_transactions(filename):
filename = "/Users/parinibrahma/Desktop/Expenditure/Credit/eStmt_2018-06-15.pdf"
reader = PyPDF2.PdfFileReader(open(filename, mode='rb' ))
n = reader.getNumPages()             # to get the count of  umber of pages in a PDF
print('no of pages are ' + str (n))
header= ["Transaction_date","Posting_date","Description","Reference_number","Account_number","Amount","Total"]
schema= StructType([StructField(col,StringType()) for col in header])
union_df = spark.createDataFrame([],schema)

for i in range(3,n+1):

            print('page no in processing ' + str (i))

            try:
                df1 =read_pdf(filename,pages=i)


                numof_coloummns=len(df1.columns)
                print(str(numof_coloummns) + '  numof_coloummns')

                try:

                    # df2 =read_pdf(filename,pages=4)

                    if numof_coloummns==7:

                        headers= ["Transaction_date","Posting_date","Description","Reference_number","Account_number","Amount","Total"]
                        schema= StructType([StructField(col,StringType()) for col in headers])
                    elif numof_coloummns== 8:

                         headers = ["Transaction_date", "Posting_date", "Description", "other", "Reference_number",
                                     "Account_number", "Amount", "Total"]
                         schema= StructType([StructField(col,StringType()) for col in headers])

                    df1= spark.createDataFrame(df1,schema=schema)

                    if numof_coloummns == 8:
                        df1 = df1.drop("other")
                        # .where("Transaction_date like '%%/%%'")

                    union_df = df1.select(*header).union(union_df.select(*header))
                    print(union_df.count())

                    union_df.show()
                    # df1.show(50)
                except ValueError:
                    pass


            except AttributeError:
                pass

union_df= union_df.where("Transaction_date like '%%/%%'")
union_df = union_df.withColumn("Transaction_date",F.concat(F.col('Transaction_date'),F.lit('/18')))
union_df.show(80)

trans_header= ["Transaction_date","Posting_date","Description","Reference_number","Account_number","Amount","Total"]
trans_schema= StructType([StructField(col,StringType()) for col in header])
master_df = spark.read.csv("/Users/parinibrahma/Desktop/Expenditure/bofa_cc_transactions.csv",header=True,schema=trans_schema)

master_df.show()

FINAL_DF = scd1(union_df,master_df)
FINAL_DF.coalesce(1).write.format("csv").mode("overwrite").save(
    "/Users/parinibrahma/Desktop/Expenditure/bofa_cc_transactions.csv",header="true")
exit()











distinctDesc_df = union_df.select('Description').distinct()
distinctDesc_df.show()

#union_df.coalesce(1).write.option("header", "true").mode("append").csv("/Users/parinibrahma/Desktop/Expenditure/descriptions/descriptions.csv")
distinctDesc_df.coalesce(1).write.format("csv").mode("append").save("/Users/parinibrahma/Desktop/Expenditure/descriptions/")
print(distinctDesc_df.count())

#union_df= union_df.select(union_df.Description,F.when(union_df.Description like "", 1).when(union_df.age < 3, -1).otherwise(0))

# whole_df.show(100)   xvtbkgcph2jrv79



schema_CSV = StructType([
    StructField("Descriptions", StringType()),
    StructField("FLAG", StringType()),
])


s_Flag_df = spark.read.csv('/Users/parinibrahma/Desktop/Expenditure/Flags_EDITED.csv', header=True, schema=schema_CSV)


Flag_values_df = s_Flag_df.select('Descriptions')
Flag_df = [str(i.Descriptions) for i in Flag_values_df.collect()]
print(Flag_df)
#Flag_df = [int(i.Descriptions) for i in Flag_df.collect()]

#Flag_df.show()

x= ['WALGREENS']

# new_column = when(
#     x[0].isin(F.col("Description")), 1).otherwise(0)

# flagged_df = union_df.withColumn('Flag',when(
#                       F.col("Description").isin(x), 1).otherwise(0))

#df.loc[df['sport'].str.contains('ball', case=False), 'sport'] = 'ball sport'
#df.sport = df.sport.apply(lambda x: 'ball sport' if 'ball' in x else x)

#union_df.loc[union_df['Description'].str.contains('WALGREENS', case=False), 'Description'] = 'WALGREENS 123'

#flagged_df = union_df.withColumn('Flag',bool(re.search(r'\bWALGREENS\b',F.col("Description"))))

#
# flagged_df = union_df.withColumn('new_col',when(
#                        F.col("Description").isin(Flag_df), 1).otherwise(0))


columns = ['Transaction_date', 'Posting_date', 'Description', 'Reference_number', 'Account_number', 'Amount', 'Total']

pandas_uniondf = union_df.toPandas()

for i in Flag_df:
        pandas_uniondf.loc[pandas_uniondf['Description'].str.contains(i, case=False), 'new_Description'] = i

# pandas_uniondf = pandas_uniondf['Description']
# print ('pandas_uniondf')
# print (pandas_uniondf)

headers= ["Transaction_date","Posting_date","Description","Reference_number","Account_number","Amount","Total","new_Description"]
schema= StructType([StructField(col,StringType()) for col in headers])

flagged_uniondf= spark.createDataFrame(pandas_uniondf,schema=schema)


u = flagged_uniondf.alias('u')
f = s_Flag_df.alias('f')


flagged_uniondf = u.join(f, u.new_Description == f.Descriptions, "left_outer")


ent_df = flagged_uniondf.filter("FLAG='E'")
ent_df.show(100)


null_flags = flagged_uniondf.filter("FLAG is NULL")
null_flags.show()

GROUPED_DF =  flagged_uniondf.groupby('FLAG').agg( F.sum("Amount"))

#flagged_uniondf = flagged_uniondf.alias('u').join(s_Flag_df.alias('f'), F.col("u.new_Description") == F.col("f.Descriptions"), "left_outer")
print ('GROUPED_DF')
GROUPED_DF.show()

#pandas_uniondf.loc[pandas_uniondf['Description'].str.contains('WALGREENS', case=False), 'Description'] = 'WALGREENS 123'




#result = union_df.withColumn('Flag', regexp_extract(F.col('Description'), 'WALGREENS', 1))




#union_df = union_df['Description'].like('%WALGREENS%')


#for row in Flag_df.rdd.collect():





#result_df= union_df.alias('u').join(Flag_df.alias('f'),F.col("u.Description") == F.col("f.Description"),"left_outer").select('u.*',"f.FLAG")



#result_df= union_df.join(Flag_df,"Description","left_outer")#.select("f.*")


print('result df')




