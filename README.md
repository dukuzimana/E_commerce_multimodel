# Distributed Multi-Model Analytics for E-commerce Data

**Course:** AUCA – Big Data Analytics
**Project Type:** Individual Final Project
**Student:** 101021 Dukuzimana Dismas
**Date:** January 2026

---

## 1. Project Overview

This project implements a **distributed multi-model analytics system** for large-scale e-commerce data using **MongoDB**, **HBase**, and **Apache Spark**. The goal is to demonstrate how different NoSQL data models and distributed processing engines can be strategically combined to answer complex business questions in an e-commerce context.

The system leverages:

* **MongoDB (Document Model)** for flexible, nested, business-oriented data
* **HBase (Wide-Column Model)** for high-volume, time-series and sparse event data
* **Apache Spark** for scalable batch analytics and cross-data-source integration

---

## 2. System Architecture

he Dataset Generator in Python creates multiple JSON files, including Users, Products, Categories, Sessions, and Transactions.

These JSON files can then be stored in two types of databases:

MongoDB, which stores the data as documents,

HBase, which stores the data in a wide-column format.

Both MongoDB and HBase feed data into Apache Spark for batch analytics and integration.

Finally, the processed data from Spark is used for visualizations and insights.


## 3. Dataset Description

The dataset is synthetically generated using **dataset_generator.py** and simulates **90 days of e-commerce activity**.

### Entities

* **Users** – demographic and registration data
* **Categories & Subcategories** – hierarchical product classification
* **Products** – catalog, inventory, and price history
* **Sessions** – detailed user browsing behavior (chunked JSON files)
* **Transactions** – purchase records linked to users and sessions

### Key Relationships

* User → Sessions → Transactions
* Products → Categories → Subcategories
* Sessions → Cart → Transactions
* Inventory updates driven by completed transactions


## 4. Technology Stack

| Technology             | Purpose                                    |
| ---------------------- | ------------------------------------------ |
| Python                 | Data generation, ingestion, analytics      |
| MongoDB                | Document storage & aggregation analytics   |
| HBase                  | Time-series and sparse event storage       |
| Apache Spark (PySpark) | Distributed batch processing & integration |
| Matplotlib / Seaborn   | Data visualization                         |
| Docker                 | Containerized database deployment          |


## 5. Project Structure

The project is named AUCA-BigData-Ecommerce-Analytics. Its main components are organized as follows:

dataset_generator.py – the Python script used to generate datasets.

data/ – a folder containing the generated JSON files:

users.json – user data

categories.json – product categories

products.json – product information

sessions_0.json and sessions_1.json – session data split into chunks

transactions.json – transaction records

mongodb/ – contains scripts and documentation for MongoDB:

load_data.py – script to load data into MongoDB

aggregations.js – MongoDB aggregation queries

schema_design.md – document describing MongoDB schema design

hbase/ – contains scripts and queries for HBase:

create_tables.hbase – HBase table creation script

load_sessions.py – script to load session data into HBase

sample_queries.hbase – example HBase queries

spark/ – scripts for Apache Spark processing and analysis:

batch_processing.py – batch processing scripts

spark_sql_analysis.py – Spark SQL queries for analysis

integration_queries.py – scripts for integrating data from multiple sources

visualizations/ – scripts for generating insights and visualizations:

sales_trends.py – sales trend analysis

customer_segments.py – customer segmentation analysis

conversion_funnel.py – conversion funnel analysis

report/ – contains project documentation:

Technical_Report.pdf – final technical report

README.md – project overview and instructions.

## 6. Part 1: Data Modeling and Storage

### 6.1 MongoDB

**Stored Data:**

* Products (with embedded price history)
* Users (profile information)
* Transactions (with embedded line items)

**Why MongoDB?**

* Supports nested and hierarchical data
* Ideal for aggregation pipelines
* Flexible schema for evolving analytics

**Implemented Analytics:**

* Top-selling products
* Revenue by category
* User purchasing frequency segmentation

### 6.2 HBase

**Stored Data:**

* Time-series user session activity
* Product interaction metrics over time

**Schema Design:**

* Row Key: `user_id#timestamp` or `product_id#date`
* Column Families:

  * `session_info`
  * `page_views`
  * `metrics`

**Why HBase?**

* Efficient for large-scale, sparse, time-ordered data
* Fast range scans for user activity streams


## 7. Part 2: Data Processing with Apache Spark

### Batch Processing

* Data cleaning and normalization
* Handling missing values
* JSON flattening and schema standardization

### Analytics Implemented

* Product co-purchase analysis
* User cohort analysis by registration month

### Spark SQL

* SQL-based analytics on DataFrames
* Joins across users, sessions, and transactions


## 8. Part 3: Analytics Integration

### Example Integrated Analysis: Customer Lifetime Value (CLV)

**Business Question:**

> Which users generate the highest long-term value?

**Data Sources:**

* MongoDB: user profiles & transactions
* HBase: session frequency and duration
* Spark: joining and aggregation

**Workflow:**

1. Load MongoDB and HBase data into Spark
2. Aggregate transaction totals per user
3. Enrich with engagement metrics
4. Compute CLV estimates


## 9. Part 4: Visualization and Insights

### Visualizations Included

* Sales performance over time
* Revenue by product category
* Customer segmentation by spending
* Conversion funnel analysis

**Key Insights:**

* A small percentage of users generate a large share of revenue
* High session engagement strongly correlates with conversion
* Certain categories show strong seasonal trends


## 10. Scalability Considerations

* MongoDB supports horizontal scaling via sharding
* HBase is designed for petabyte-scale time-series data
* Spark enables distributed computation across clusters


## 11. How to Run the Project

1. Generate dataset:

```bash
pip install faker pandas
python dataset_generator.py
```

2. Start MongoDB and HBase (Docker recommended)

3. Load data into MongoDB and HBase

4. Run Spark analytics:

```bash
spark-submit spark/batch_processing.py
```

5. Generate visualizations


## 12. Limitations and Future Work

* Streaming analytics can be extended using Spark Structured Streaming
* Recommendation models can be improved using MLlib
* Real-time dashboards can be added


## 13. Conclusion

This project demonstrates how **multi-model databases and distributed processing** can be effectively combined to solve complex e-commerce analytics problems. Each technology is used where it fits best, resulting in a scalable, flexible, and insightful analytics architecture.


**Done by 101021 Dukuzimana Dismas**
