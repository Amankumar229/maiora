# ETL Sales Data Pipeline

## Description
This project is an ETL pipeline that:
- Extracts sales data from two CSV files from different regions.
- Transforms the data based on business rules.
- Loads the transformed data into an SQLite database.
- Validates the data using SQL queries.

---

## Features
- Combines sales data from two regions (A and B) into a single table.
- Calculates total sales and net sales after discounts.
- Removes duplicate orders and filters negative or zero sales.
- Loads cleaned data into SQLite.
- Validates data with SQL queries for:
  - Total number of records.
  - Total sales amount by region.
  - Average sales per transaction.
  - Ensures no duplicate `OrderId`s.

---

## Prerequisites
- Python 3.x
- SQLite

---

## Installation

1. **Clone the Repository:**
```sh
git clone https://github.com/YOUR_USERNAME/etl-sales-data-pipeline.git
cd etl-sales-data-pipeline
