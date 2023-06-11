from src.assignment4.utils import *

#calling a function to create a spark session
spark = create_sparkseesion()

#calling a function to create a employee dataframe
df_employee=create_dataframe(spark)
print("Employee table")
df_employee.show(truncate=False)

#calling a function to return the first row from each deaprtment group
df_first_row = first_row(df_employee)
print("Employee table with row number column")
df_first_row.show(truncate=False)

#calling a function to retrieve employees who have highest salary from each dept group
df_highest_salary = highest_salary(df_employee)
print("Employee table displaying employees with highest salary")
df_highest_salary.show(truncate=False)

#calling a function to display highest, lowest, average, and total salary for each department group
df_emp_salary = emp_salary(df_employee)
print("Employee table with highest,lowest,average and total salary for each department")
df_emp_salary.show(truncate=False)