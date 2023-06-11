from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,when,lit,expr,max

def create_sparkseesion():
    spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment1 on pyspark+sql")\
                  .getOrCreate()
    return spark

def employee_dataframe(spark):
    emp_data=[({"firstname":"James","middlename":"","lastname":"Smith"}, "03011998", "M", 3000),
              ({"firstname":"Michael","middlename":"Rose","lastname":""}, "10111998", "M", 20000),
              ({"firstname":"Robert","middlename":"","lastname":"Williams"}, "02012000", "M", 3000),
              ({"firstname":"Maria","middlename":"Anne","lastname":"Jones"}, "03011998", "F", 11000),
              ({"firstname":"Jen","middlename":"Mary","lastname":"Brown"}, "04101998", "F", 10000)
             ]
    emp_schema=StructType([
                    StructField("name", MapType(StringType(), StringType()), True),
                    StructField("dob", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("salary",IntegerType(), True)
                         ])
    employee_df=spark.createDataFrame(data=emp_data,schema=emp_schema)
    return employee_df
#
def select_column(employee_df):
    select_column_df= employee_df.select(col("name.firstname"),
                          col("name.lastname"),
                          col("salary")
                            )
    return select_column_df

def add_column(employee_df):
    country_column_df = employee_df.withColumn("Country", lit("India"))
    department_column_df =country_column_df.withColumn("Department", lit("Finance"))
    age_column_df = department_column_df.withColumn("Age",
                            when(col("name.firstname") == "James", lit("29")).
                            when(col("name.firstname") == "Michael", lit("30")).
                            when(col("name.firstname") == "Robert", lit("25")).
                            when(col("name.firstname") == "Maria", lit("28")).
                            otherwise(lit("26")))
    return age_column_df

def modify_salary_column(employee_df):
    salary_new_df=employee_df.withColumn("salary_new",(col("salary")*3))
    return salary_new_df

def modify_datatype(employee_df):
    modify_datatype_df=employee_df.withColumn("dob", col("dob").cast(StringType()))\
                               .withColumn("salary", col("salary").cast(StringType()))
    return modify_datatype_df

def new_salary_column(employee_df):
    new_salary_df = employee_df.withColumn("salary_new1", (col("salary") * 2))
    return new_salary_df

def rename_columns(employee_df):
    rename_columns_df = employee_df.withColumn("name",expr("map('firstposition' , name['firstname'], 'middleposition', name['middlename'], 'lastposition', name['lastname'])"))
    return rename_columns_df

def max_salary(employee_df):
    max_salary_df = employee_df.select(col("name.firstname")).filter(col("salary") == 20000)
    return max_salary_df

def drop_column(age_column_df):
    drop_column_df = age_column_df.drop("Department").drop("Age")
    return drop_column_df

def distinct_value(employee_df):
    distinct_dob_df = employee_df.select("dob").distinct()
    distinct_salary_df = employee_df.select("salary").distinct()
    return distinct_dob_df,distinct_salary_df