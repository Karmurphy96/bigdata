**End-to-End Big Data Pipeline**
This project was developed as part of the MSc Data Analytics (Big Data Analytics module) at the Berlin School of Business & Innovation.
It demonstrates the design and implementation of an end-to-end Big Data pipeline using Hadoop, Spark, Hive, and MLlib on the Amazon Reviews Dataset (>5GB).

**Project Overview**
The goal of this project was to build a scalable data pipeline that can ingest, process, and analyze large-scale datasets, followed by applying machine learning for sentiment classification.

**Key Features**
**Data Ingestion & Storage**

Converted raw JSONL files to CSV format.

Stored and managed data in HDFS-compatible storage.

Created Hive external tables for structured access.

**Data Processing**

Applied ETL with PySpark for cleaning, wrangling, and transformations.

Ran HiveQL queries for exploratory data analysis and performance benchmarking against Spark SQL.

**Machine Learning**

Implemented Logistic Regression (MLlib) for sentiment classification.

Evaluated using Accuracy, Precision, Recall, F1-score, and ROC-AUC.

Visualized results with a Confusion Matrix.

**Performance Comparison**

Benchmarked Spark vs Hive for specific query and transformation tasks.

**Technologies Used**
Hadoop (HDFS)

Apache Spark (PySpark, MLlib)

Apache Hive

Python

Google Cloud Dataproc / GCS (for deployment)
