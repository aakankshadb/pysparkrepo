from src.assignment2.utils import *

spark=create_sparkseesion()

df_employee = employee_dataframe(spark)
print("Employee Table")
df_employee.show(truncate=False)

df_select_column = select_column(df_employee)
print("Table with selected Columns")
df_select_column.show(truncate=False)

df_add_column = add_column(df_employee)
print("Employee Table with Country Column")
df_add_column.show(truncate=False)

df_modify_salary_column = modify_salary_column(df_employee)
print("Employee Table with new Salary Column")
df_modify_salary_column.show(truncate=False)

df_new_datatype=modify_datatype(df_employee)
print("Schema for the Employee Table after modifying the dob and salary datatype")
df_new_datatype.printSchema()

df_new_salary_column = new_salary_column(df_employee)
print("Employee table with new column from salary column")
df_new_salary_column.show(truncate=False)

df_rename_columns = rename_columns(df_employee)
print("Employee table with new column names")
df_rename_columns.show(truncate=False)

df_max_salary = max_salary(df_employee)
print("Name with maximum salary")
df_max_salary.show(truncate=False)

df_drop_column = drop_column(df_add_column)
print("Employee table after dropping department and age column")
df_drop_column.show(truncate=False)

df_distinct_dob,df_distinct_salary = distinct_value(df_employee)
print("Employee Table with distinct dob and salary values")
df_distinct_dob.show(truncate=False)
df_distinct_salary.show(truncate=False)
