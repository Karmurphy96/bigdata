# Step 0: Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("ReviewClassification").getOrCreate()

# Step 2: Load Cleaned Dataset from GCS
gcs_path = "gs://bigdataecommerce/reviews_data/cleaned_reviews_output/"
df = spark.read.csv(gcs_path, header=True, inferSchema=True)
df.select("rating", "title").show(5)

# Step 3: Create Label Column (1 if rating >= 4 else 0)
df = df.withColumn("label", when(df.rating >= 4, 1).otherwise(0))
df.select("rating", "label").show(5)

# Step 4: Tokenize Title Text
tokenizer = Tokenizer(inputCol="title", outputCol="words")
df_tokenized = tokenizer.transform(df)
df_tokenized.select("title", "words").show(5, truncate=False)

# Step 5: Convert Words to Feature Vectors using HashingTF
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
df_featurized = hashingTF.transform(df_tokenized)
df_featurized.select("words", "rawFeatures").show(5, truncate=False)

# Step 6: Apply IDF to Get TF-IDF Features
idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(df_featurized)
df_rescaled = idf_model.transform(df_featurized)
df_rescaled.select("features", "label").show(5, truncate=False)

# Step 7: Train-Test Split
train_data, test_data = df_rescaled.randomSplit([0.8, 0.2], seed=42)

# Step 8: Train Logistic Regression Model
lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)

# Step 9: Make Predictions
predictions = lr_model.transform(test_data)
predictions.select("label", "prediction", "probability").show(5, truncate=False)

# Step 10: Evaluate Performance Metrics
evaluator_auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
evaluator_multi = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

auc = evaluator_auc.evaluate(predictions)
accuracy = evaluator_multi.setMetricName("accuracy").evaluate(predictions)
precision = evaluator_multi.setMetricName("weightedPrecision").evaluate(predictions)
recall = evaluator_multi.setMetricName("weightedRecall").evaluate(predictions)
f1 = evaluator_multi.setMetricName("f1").evaluate(predictions)

print(f"AUC: {auc:.4f}")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1-Score: {f1:.4f}")

# Step 11: Confusion Matrix Visualization
preds_pd = predictions.select("label", "prediction").toPandas()
conf_matrix = pd.crosstab(preds_pd["label"], preds_pd["prediction"], rownames=["Actual"], colnames=["Predicted"])

# Plot
plt.figure(figsize=(6, 4))
sns.heatmap(conf_matrix, annot=True, cmap="Blues", fmt="g")
plt.title("Confusion Matrix - Logistic Regression")
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.tight_layout()

# Save plot to GCS-compatible path
plt.savefig("/tmp/classification_confusion_matrix.png")
plt.show()

# Step 12 (in terminal): Upload image to GCS
gsutil cp /tmp/classification_confusion_matrix.png gs://bigdataecommerce/reviews_data/








