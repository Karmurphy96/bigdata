from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Create Spark session
spark = SparkSession.builder.appName("CompareHiveSpark").getOrCreate()

# Path to cleaned CSV file in GCS
path = "gs://bigdataecommerce/reviews_data/cleaned_reviews_output/"

# Read CSV from GCS
df = spark.read.csv(path, header=True, inferSchema=True)

# Total number of reviews
df.count()

# Show average rating overall
df.select(avg("rating")).show()

# Top 10 products by average rating
df.groupBy("asin").agg(avg("rating").alias("avg_rating")) \
  .orderBy("avg_rating", ascending=False).show(10, truncate=False)

# Top 10 users by number of reviews
df.groupBy("user_id").agg(count("*").alias("review_count")) \
  .orderBy("review_count", ascending=False).show(10, truncate=False)
