from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, col, when, count, isnan

# Step 1: Start the Spark session
spark = SparkSession.builder.appName("CleanBeautyReviews").getOrCreate()

# Step 2: We load the flattened CSV
df = spark.read.csv("gs://bigdataecommerce/reviews_data/flattened_reviews.csv", header=True, inferSchema=True)

# Step 3: To show nulls
df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()

# Step 4: Drop rows with nulls in important fields
df_clean = df.dropna(subset=["rating", "title", "text", "user_id"])

# Step 5: To add review time and year
df_clean = df_clean.withColumn("review_time", from_unixtime(col("timestamp") / 1000))
df_clean = df_clean.withColumn("review_year", year(col("review_time")))

# Step 6: Save to subfolder under reviews_data
output_path = "gs://bigdataecommerce/reviews_data/cleaned_reviews_output"
df_clean.write.csv(output_path, header=True, mode="overwrite")

# Optional log
print("Saved cleaned data to:", output_path)
print("Clean row count:", df_clean.count())
