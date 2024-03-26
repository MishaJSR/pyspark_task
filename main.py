from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("test").getOrCreate()

data = [
    Row(product_name='Product A', category_name='Category 1'),
    Row(product_name='Product B', category_name='Category 2'),
    Row(product_name='Product C', category_name=None),
    Row(product_name='Product D', category_name='Category 1'),
]

df = spark.createDataFrame(data)


def get_product_category(df):
    pairs_df = df.select("product_name", "category_name")
    products_with_no_category = df.filter(col("category_name").isNull()).select("product_name").distinct()
    return pairs_df, products_with_no_category


pairs_df, products_with_no_category = get_product_category(df)

print("Product-Category Pairs:")
pairs_df.show()
print("\nProducts with No Category:")
products_with_no_category.show()
