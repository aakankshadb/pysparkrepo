from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

#creating function to create a spark session object
def create_sparkseesion():
    spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment on pyspark")\
                  .getOrCreate()
    return spark

#creating a dataframe for product table
def product_dataframe(spark):
    product_schema=StructType([StructField(name='ProductName',dataType=StringType()),
                          StructField(name='Issue Date',dataType=StringType()),
                          StructField(name='Price',dataType=IntegerType()),
                          StructField(name='Brand',dataType=StringType()),
                          StructField(name='Country',dataType=StringType()),
                          StructField(name='Product_Number',dataType=IntegerType())
                          ])
    product_df = spark\
            .read\
            .format('csv')\
            .options(header=True)\
            .schema(product_schema)\
            .load("../../resource/product.csv")
    return product_df

#creating a function to convert  the issue date to timestamp format.
def issue_date_timestamp(product_df):
    df_timestamp_string = product_df.withColumn("Issue_Date_string",F.from_unixtime(F.col("Issue Date")/1000,"yyyy-MM-dd'T'HH:mm:ssZZZZ"))
    df_timestamp = df_timestamp_string.withColumn("Issue_Date_Timestamp",F.to_timestamp(F.col("Issue_Date_string")))
    df_timestamp.printSchema()
    return df_timestamp

# creating a function to convert  the issue date from timestamp to datestamp format.
def issue_date_datestamp(df_timestamp):
    df_date = df_timestamp.withColumn("Date",F.to_date(F.col("Issue_Date_Timestamp"),"yyyy-mm-dd"))
    df_date.printSchema()
    return df_date

#creating a function to remove starting space in Brand Column.
def remove_space(df_date):
    df_remove_space = df_date.withColumn("Brand_new",F.trim(F.col("Brand")))
    return df_remove_space

#creating a function to replace null values with empty spaces Country Column.
def replace_null(df_remove_space):
    df_replace_null = df_remove_space.withColumn("Country_new",F.when(F.col("Country") == "null", '').otherwise(F.col("Country")))
    return df_replace_null
#
# transaction_schema=StructType([StructField(name='SourceId',dataType=IntegerType()),
#                           StructField(name='TransactionNumber',dataType=IntegerType()),
#                           StructField(name='Language',dataType=StringType()),
#                           StructField(name='ModelNumber',dataType=IntegerType()),
#                           StructField(name='StartTime',dataType=StringType()),
#                           StructField(name='ProductNumber',dataType=IntegerType())
#                           ])
# transaction_df = spark\
#             .read\
#             .format('csv')\
#             .options(header=True)\
#             .schema(transaction_schema)\
#             .load("../../resource/Product_transaction.csv")
#
# miliisecond_df=transaction_df.withColumn("StartTime_timestamp",F.to_timestamp(F.col("StartTime")))
# miliisecond_df1=miliisecond_df.withColumn("start_time_ms",F.unix_timestamp(F.col("StartTime_timestamp")))
# miliisecond_df1.show(truncate=False)
# joined_df=df_empty.join(miliisecond_df1, df_empty.Product_Number == miliisecond_df1.ProductNumber,"left")
# joined_df.select(F.col('ProductName'),
#                  F.col('Price'),
#                  F.col('Brand_new'),
#                  F.col('Product_Number'),
#                  F.col('Date'),
#                  F.col('Country1'),
#                  F.col('SourceId'),
#                  F.col('TransactionNumber'),
#                  F.col('Language'),
#                  F.col('ModelNumber'),
#                  F.col('start_time_ms')
#                  ).show(truncate=False)
