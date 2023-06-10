from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

#creating a spark session object
spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment on pyspark")\
                  .getOrCreate()

#creating a dataframe
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
product_df.show(truncate=False)

df_timestamp_string=product_df.withColumn("Timestamp string",F.from_unixtime(F.col("Issue Date")/1000,"yyyy-MM-dd'T'HH:mm:ssZZZZ"))
df_timestamp=df_timestamp_string.withColumn("Timestamp",F.to_timestamp(F.col("Timestamp string")))
df_date=df_timestamp.withColumn("Date",F.to_date(F.col("Timestamp"),"yyyy-mm-dd"))
df_leading_space=df_date.withColumn("Brand_new",F.trim(F.col("Brand")))
df_empty=df_leading_space.withColumn("Country1",F.when(F.col("Country") == "null", '').otherwise(F.col("Country")))
df_empty.show()
#
transaction_schema=StructType([StructField(name='SourceId',dataType=IntegerType()),
                          StructField(name='TransactionNumber',dataType=IntegerType()),
                          StructField(name='Language',dataType=StringType()),
                          StructField(name='ModelNumber',dataType=IntegerType()),
                          StructField(name='StartTime',dataType=StringType()),
                          StructField(name='ProductNumber',dataType=IntegerType())
                          ])
transaction_df = spark\
            .read\
            .format('csv')\
            .options(header=True)\
            .schema(transaction_schema)\
            .load("../../resource/Product_transaction.csv")

miliisecond_df=transaction_df.withColumn("StartTime_timestamp",F.to_timestamp(F.col("StartTime")))
miliisecond_df1=miliisecond_df.withColumn("start_time_ms",F.unix_timestamp(F.col("StartTime_timestamp")))
miliisecond_df1.show(truncate=False)
joined_df=df_empty.join(miliisecond_df1, df_empty.Product_Number == miliisecond_df1.ProductNumber,"left")
joined_df.select(F.col('ProductName'),
                 F.col('Price'),
                 F.col('Brand_new'),
                 F.col('Product_Number'),
                 F.col('Date'),
                 F.col('Country1'),
                 F.col('SourceId'),
                 F.col('TransactionNumber'),
                 F.col('Language'),
                 F.col('ModelNumber'),
                 F.col('start_time_ms')
                 ).show(truncate=False)
