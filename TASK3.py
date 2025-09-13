from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round

# Spark Session
spark: SparkSession = SparkSession.builder \
    .appName("Data Analysis in PySpark") \
    .getOrCreate()


users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("users.csv")
purchases_df = spark.read.option("header", "true").option("inferSchema", "true").csv("purchases.csv")
products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("products.csv")

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Merge purchases with products
purchases_with_products = purchases_df.join(products_df, on="product_id")

# Adding column: quantity * price
purchases_with_products = purchases_with_products.withColumn("total", col("quantity") * col("price"))

# Total amount
total_by_category = purchases_with_products.groupBy("category").agg(_sum("total").alias("total_sum"))

total_by_category.show()

# Full data
full_data = purchases_with_products.join(users_df, on="user_id")

# Filtered users
age_filtered = full_data.filter((col("age") >= 18) & (col("age") <= 25))

# Categories - groups
total_18_25_by_category = age_filtered.groupBy("category").agg(_sum("total").alias("age_group_total"))

total_18_25_by_category.show()

# Total amount for age categories
total_18_25_sum = age_filtered.agg(_sum("total").alias("total")).collect()[0]["total"]

# Calculate percentage
percentage_df = total_18_25_by_category.withColumn(
    "percentage",
    round((col("age_group_total") / total_18_25_sum) * 100, 2)
)

percentage_df.show()

# Highest percentage
top_3_categories = percentage_df.orderBy(col("percentage").desc()).limit(3)

top_3_categories.show()