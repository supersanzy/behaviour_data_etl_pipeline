# Behavior Dataset ETL Pipeline

## Overview
This project is a scalable data engineering pipeline built using VS Code, Python, Spark, and PostgreSQL.

The pipeline processes behavioral event and order datasets from CSV files, transforms the data using the Medallion Architecture (Bronze → Silver → Gold), and performs business analytics on user activities and order trends.

---

# Architecture

## Medallion Architecture

The project follows the Medallion Architecture pattern:

### Bronze Layer
- Raw CSV datasets are ingested using Spark
- Multiple datasets are merged using `unionByName()`
- Data is stored in Parquet format for optimized storage and faster querying

### Silver Layer
- Data cleaning and transformation occur here
- Standardized schemas and refined datasets
- Stored in Parquet
- Partitioned by:
  - `year`

### Gold Layer
- Business-ready analytical datasets
- Aggregated and modeled for reporting and insights
- Stored in Parquet
- Partitioned by:
  - `year`
  - `month`

---

# Tech Stack

| Technology | Purpose |
|---|---|
| VS Code | Development Environment |
| Python | ETL scripting |
| Spark | Distributed data processing |
| PostgreSQL | Data warehouse / analytics storage |
| JDBC | Spark-to-PostgreSQL connectivity |
| Parquet | Columnar storage format |
| Matplotlib | Data visualization |
| Seaborn | Statistical visualization |
| Jupyter Notebook | Data validation and analysis |
| Medallion Architecture | Data modeling strategy |

---

# Dataset Source

The dataset source consists of CSV files containing:
- User behavioral events
- Product information
- Orders data

Spark was used to:
1. Read the CSV files
2. Merge datasets using `unionByName()`
3. Transform and clean the data
4. Write optimized Parquet files

---

# Data Model

## Fact Tables

### `fact_orders`
Contains transactional order data such as:
- Orders
- Revenue
- Purchase activities
- Sales metrics

### `fact_user_events`
Contains user behavioral event data such as:
- Product views
- Add-to-cart events
- Purchases
- User interactions

---

## Dimension Tables

### `dim_products`
Contains product-related information.

### `dim_users`
Contains user-related information.

### `dim_events`
Contains event metadata and event classifications.

---

# PostgreSQL Integration

The Gold Layer datasets were loaded into PostgreSQL using Spark JDBC connectivity.

## JDBC Connection
Spark connected to PostgreSQL through the PostgreSQL JDBC Driver to enable efficient loading of analytical tables into the database.

### Features
- Spark-to-PostgreSQL integration using JDBC
- Batch loading of Gold Layer tables
- Optimized analytical storage
- Support for scalable querying and reporting

### Tables Loaded into PostgreSQL
- `fact_orders`
- `fact_user_events`
- `dim_products`
- `dim_users`
- `dim_events`

---

# Data Processing Workflow

## 1. Data Ingestion
- CSV files loaded into Spark DataFrames

## 2. Data Merging
- Combined datasets using:
```python
unionByName()
```

## 3. Bronze Storage
- Raw datasets written as Parquet

## 4. Silver Transformations
- Data cleaning
- Schema standardization
- Partitioning by year

## 5. Gold Modeling
- Analytical modeling
- Fact and dimension table creation
- Partitioning by year and month

## 6. PostgreSQL Loading
- Gold Layer tables loaded into PostgreSQL using JDBC

---

# Data Quality Validation

Data quality validation tests were performed to ensure:
- Data consistency
- Schema validation
- Null value checks
- Duplicate checks
- Data accuracy

The validation scripts were written and saved in Jupyter Notebook (`.ipynb`) files.

---

# Business Analysis & Visualization

Business logic analysis was performed on the Gold Layer datasets to generate insights and trends.

The analysis includes:
- User behavioral patterns
- Purchase trends
- Product engagement
- Revenue analysis
- Customer interaction metrics

Dashboards and visualizations were created using:
- Matplotlib
- Seaborn

These charts were designed to provide visually appealing and easy-to-understand business insights.

## Visualization Output
- Dashboard, data lineage and ER relationship images are stored in the `images/` folder
- Business logic answers and analysis notebooks are saved as `.ipynb` files

---

# Partitioning Strategy

| Layer | Partition |
|---|---|
| Silver | `year` |
| Gold | `year`, `month` |

This improves:
- Query performance
- Data pruning
- Scalability
- Optimized reads

---

# Example Business Questions

- Which Top 5 brands generated the highest sales?
- Which users interact most frequently with products?
- What are the most common event types?
- Which month and year generated the highest sales?
- Users that viewed a product, added the product to a cart and purchased that same product?
- Users that bought more than 1 products?

---

# Project Structure

```bash
/medallion_architecture/
│
├── bronze_layer/
├── silver_layer/
├── gold_layer/
/python_scripts/
│
├── bronze_layer.py
├── silver_layer.py
├── gold_layer.py
├── load_to_postgres.py
/
├── business_logic.ipynb
├── main.py
├── quality_test.ipynb
└── README.md
```

---

# Key Features

- Distributed data processing with Spark
- Optimized Parquet storage
- Medallion Architecture implementation
- Fact and dimension modeling
- Partitioned datasets
- Behavioral analytics
- Scalable ETL design
- PostgreSQL integration using JDBC
- Data quality validation tests
- Business dashboards and visualizations
- Analytical reporting using Jupyter Notebook

---

# Future Improvements

- Add orchestration using Airflow
- Dockerize the pipeline
- Implement CI/CD
- Add real-time streaming ingestion
- Build interactive dashboards using Power BI or Tableau
- Automate data quality testing

---

# Conclusion

This project demonstrates a modern data engineering workflow using Spark and PostgreSQL to process behavioral datasets at scale. The implementation of the Medallion Architecture, fact/dimension modeling, partitioned Parquet storage, JDBC integration, data validation testing, and business intelligence dashboards enables efficient analytics and scalable data processing.