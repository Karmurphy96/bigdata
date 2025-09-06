**End-to-End Big Data Pipeline**

This repository contains the implementation of my MSc Data Analytics – Big Data Analytics module project. The project demonstrates the development of an end-to-end Big Data pipeline using Hadoop, Spark, Hive, and MLlib on the Amazon Reviews Dataset (>5GB).

Project Overview

1. The goal of this project was to design and implement a scalable data pipeline that can:

2. Ingest and store raw data.

3. Process and transform data using Spark & Hive.

4. Apply machine learning with MLlib for sentiment classification.

5. Compare performance between Spark and Hive.

**Key Features**
**Data Ingestion & Storage**

1. Converted raw JSONL files to CSV format.

2. Stored and managed data in HDFS-compatible storage.

3. Created Hive external tables for structured access.

**Data Processing**

1. Applied ETL with PySpark for cleaning, wrangling, and transformations.

2. Ran HiveQL queries for exploratory data analysis and performance benchmarking against Spark SQL.

**Machine Learning**

1. Implemented Logistic Regression (MLlib) for sentiment classification.

2. Evaluated using Accuracy, Precision, Recall, F1-score, and ROC-AUC.

3. Visualized results with a Confusion Matrix.

**Performance Comparison**

Benchmarked Spark vs Hive for specific query and transformation tasks.

**Technologies Used**

Hadoop (HDFS)

Apache Spark (PySpark, MLlib)

Apache Hive

Python

Google Cloud Dataproc / GCS (for deployment)

**Results**

Built a fully functional pipeline that processed 5GB+ of Amazon Review data.

Achieved strong classification results (e.g., ROC-AUC > 0.80).

Reduced query execution time by leveraging Spark optimizations over Hive.


