from src.assignment3.utils import *

spark=create_sparkseesion()

df_product = create_dataframe(spark)
print("Product Table")
df_product.show(truncate=False)

df_pivot = pivot_data(df_product)
print("Product table after using Pivot function")
df_pivot.show(truncate=False)

df_unpivot = unpivot_data(df_pivot)
print("Product table after using unpivot function")
df_unpivot.show(truncate=False)