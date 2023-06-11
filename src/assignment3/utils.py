from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col,expr

def create_sparkseesion():
    spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment2 on pyspark+sql")\
                  .getOrCreate()
    return spark

def create_dataframe(spark):
    product_data=[("Banana",1000,"USA"),
                  ("Carrots", 1500, "India"),
                  ("Beans", 1600, "Sweden"),
                  ("Orange", 2000, "UK"),
                  ("Orange", 2000, "UAE"),
                  ("Banana",400,"China"),
                  ("Carrots", 1200, "China")
                  ]
    product_schema=StructType([StructField("Product", StringType(),True),
                               StructField("Amount", IntegerType(),True),
                               StructField("Country", StringType(),True)
                              ])
    product_df=spark.createDataFrame(data=product_data,schema=product_schema)
    return product_df

def pivot_data(product_df):
    pivot_df = product_df.groupBy("Product").pivot("Country").sum("Amount")
    return pivot_df

def unpivot_data(pivot_df):
    unpivotExpr = "stack(6,'China',china,'India',india,'Sweden',sweden,'UAE',uae,'UK',uk,'USA',usa) as (Country,Total)"
    unpivot_df = pivot_df.select("Product", expr(unpivotExpr)) \
                        .where("Total is not null")
    return unpivot_df