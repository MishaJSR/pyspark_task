from pyspark.sql import SparkSession
from pyspark.sql import Row

# Создаем Spark сессию
spark = SparkSession.builder.appName("example").getOrCreate()

# Пример данных для датафрейма
data = [
    Row(product_name='Product A', category_name='Category 1'),
    Row(product_name='Product B', category_name='Category 2'),
    Row(product_name='Product C', category_name=None),
    Row(product_name='Product D', category_name='Category 1'),
]

df = spark.createDataFrame(data)

def get_product_category_pairs(df):
    from pyspark.sql.functions import col
    pairs_df = df.select("product_name", "category_name")
    products_with_no_category = df.filter(col("category_name").isNull()).select("product_name").distinct()
    return pairs_df, products_with_no_category

pairs_df, products_with_no_category = get_product_category_pairs(df)

print("Product-Category Pairs:", pairs_df)
print("\nProducts with No Category:", products_with_no_category)