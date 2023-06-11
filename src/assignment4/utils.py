from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col,avg,sum,min,max

#creating function to create a spark session object
def create_sparkseesion():
    spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment on pyspark")\
                  .getOrCreate()
    return spark

##creating a function to create dataframe for employee table
def create_dataframe(spark):

    emp_data=[("James","Sales",3000),
              ("Michael","Sales",4600),
              ("Robert","Sales",4100),
              ("Maria","Finance",3000),
              ("Raman","Finance",3000),
              ("Scott","Finance",3300),
              ("Jen","Finance",3900),
              ("Jeff","Marketing",3000),
              ("Kumar","Marketing",2000)
            ]
    emp_schema = StructType([
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True)
                            ])
    emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
    return emp_df

#creating a function to select first row from each department group
def first_row(emp_df):
   windowspec = Window.partitionBy("department").orderBy("salary")
   row_df = emp_df.withColumn("row_number", row_number().over(windowspec))
   first_row_df = row_df.select("*").filter(col("row_number") == 1)
   return first_row_df

#creating a function to retrieve employees who earns the highest salary.
def highest_salary(emp_df):
   windowspec = Window.partitionBy("department").orderBy(col("salary").desc())
   ordered_df = emp_df.withColumn("row_number", row_number().over(windowspec))
   highest_salary_df = ordered_df.select("employee_name","department","salary",).filter(col("row_number") == 1)
   return highest_salary_df

#creating a function to select the highest, lowest, average, and total salary for each department group.
def emp_salary(emp_df):
    windowspec = Window.partitionBy("department")
    windowspec1= Window.partitionBy("department").orderBy(col("salary"))
    emp_salary_df = emp_df.withColumn("row_number",row_number().over(windowspec1))\
                          .withColumn("average_salary",avg(col("salary")).over(windowspec))\
                          .withColumn("total_salary", sum(col("salary")).over(windowspec)) \
                          .withColumn("min_salary", min(col("salary")).over(windowspec)) \
                          .withColumn("max_salary", max(col("salary")).over(windowspec)) \
                          .filter(col("row_number") == 1)\
                          .select("employee_name","department","salary","average_salary","total_salary","min_salary","max_salary")

    return emp_salary_df