# Data Engineer Assignment Documentation

## Requirements

1. **Load Data**:
    - Load customer transactions from a JSON file.
    - Load product catalog from a CSV file.

2. **Clean Data**:
    - Remove duplicate rows.
    - Convert timestamps to datetime objects.
    - Validate and clean the price column, ensuring all values are positive numbers.

3. **Store Data**:
    - Store raw data in the `raw` schema of a PostgreSQL database.
    - Store cleaned data in the `ingest` schema of a PostgreSQL database.

4. **Merge Data**:
    - Merge customer transactions with the product catalog on the `product_id` column.
    - Store the merged data in the `curate` schema of a PostgreSQL database.

5. **Generate Reports**:
    - **Top 2 best-selling products.**
      - Product 1 with total sale of 15 items
      - Product 3 with total sale of 6 items
    - **Average order value per customer.**
      - Customer C001 bought avg 700
      - Customer C003 bought avg 600
      - Customer C002 bought avg 600
    - **Total revenue generated per product category.**
      - Category B generated 800
      - Category A generated 2400


## Approach

### Loading Data

The data loading process involves reading data from JSON and CSV files using the `pandas` library. The `load_data` function handles this by checking the file type and using the appropriate `pandas` function to read the data into a DataFrame.

### Cleaning Data

Data cleaning is performed by the `clean_data` function, which:
- Removes duplicate rows using `drop_duplicates()`.
- Converts the `timestamp` column to datetime objects using `pd.to_datetime()`.
- Filters the `price` column to ensure all values are valid positive numbers using the `is_valid_value` function.

### Storing Data

Data is stored in different schemas of a PostgreSQL database to maintain a clear separation between raw, cleaned, and merged data. This is achieved using the `to_sql` method of the `pandas` DataFrame, specifying the target schema.

### Merging Data

The `merge` method of the `pandas` DataFrame is used to combine customer transactions with the product catalog based on the `product_id` column. This merged data is then stored in the `curate` schema.

### Generating Reports

SQL queries are executed against the `curate` schema to generate the required reports:
- Top 2 best-selling products.
- Average order value per customer.
- Total revenue generated per product category.

## Design Decisions and Justifications

1. **Use of `pandas` for Data Handling**:
    - **Justification**: `pandas` provides powerful data manipulation capabilities, making it easy to load, clean, and merge data efficiently.
  
2. **Data Cleaning Approach**:
    - **Justification**: Removing duplicates and invalid data ensures the integrity and accuracy of the dataset, which is critical for reliable analysis.
    - **Price Validation**: Ensuring that the price is a positive number filters out erroneous entries that could skew the analysis.

3. **Database Schema Separation**:
    - **Justification**: Storing data in separate schemas (`raw`, `ingest`, `curate`) allows for better organization and traceability of data transformations. It also provides a clear lineage of data processing steps.

4. **Error Handling for Database Connection**:
    - **Justification**: Implementing a retry mechanism for establishing the database connection ensures robustness in the face of temporary connectivity issues.

5. **Execution Flow**:
    - **Justification**: The script is structured to first load and store raw data, then clean and store cleaned data, followed by merging data and generating reports. This logical flow ensures that each step builds upon the previous one, maintaining data consistency throughout the process.

6. **SQL Queries for Report Generation**:
    - **Justification**: Using SQL queries to generate reports leverages the power of the PostgreSQL database for efficient data aggregation and analysis.

## Code

Below is the main script used for the data processing and analysis pipeline:

```python
import os
import pandas as pd
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

DB_CONNECTION = os.environ["DB_CONNECTION"]

def load_data(path: str, type: str) -> pd.DataFrame:
    if type == "csv":
        return pd.read_csv(path)
    elif type == "json":
        return pd.read_json(path)
    else:
        raise Exception("File extension not supported")

def is_valid_value(value):
    try:
        val = float(value)
        return val > 0
    except ValueError:
        return False

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    col_names = df.columns
    if "timestamp" in col_names:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    if "price" in col_names:
        df = df[df["price"].apply(is_valid_value)]
    return df

if __name__ == "__main__":
    while True:
        try:
            engine = create_engine(DB_CONNECTION)
            conn = engine.connect()
            conn.close()
            break
        except OperationalError as e:
            time.sleep(5)

    with engine.connect() as conn:
        customer_txn_df = load_data("./raw/customer_transactions.json", "json")
        product_cat_df = load_data("./raw/product_catalog.csv", "csv")

        customer_txn_df.to_sql(
            name="customer_transactions",
            schema="raw",
            con=conn,
            if_exists="append",
            index=False,
        )
        product_cat_df.to_sql(
            name="product_catalog",
            schema="raw",
            con=conn,
            if_exists="append",
            index=False,
        )

        clean_customer_txn_df = clean_data(customer_txn_df)
        clean_product_cat_df = clean_data(product_cat_df)

        clean_customer_txn_df.to_sql(
            name="customer_transactions",
            schema="ingest",
            con=conn,
            if_exists="append",
            index=False,
        )
        clean_product_cat_df.to_sql(
            name="product_catalog",
            schema="ingest",
            con=conn,
            if_exists="append",
            index=False,
        )

        merge_data = clean_customer_txn_df.merge(
            clean_product_cat_df, on="product_id", how="inner"
        )
        merge_data.to_sql(
            "unified_transactions",
            schema="curate",
            con=conn,
            if_exists="append",
            index=False,
        )

        query = """
        SELECT product_id, product_name, SUM(quantity) AS total_sold
        FROM curate.unified_transactions
        GROUP BY product_id, product_name
        ORDER BY total_sold DESC
        LIMIT 2;
        """
        top_2_selling = pd.read_sql(query, conn)
        print(top_2_selling)
        top_2_selling.to_csv("./output/top2_best_selling.csv", index=False)

        query = """
        SELECT customer_id, AVG(quantity * price_x) AS average_order_value
        FROM curate.unified_transactions
        GROUP BY customer_id;
        """
        avg_price_per_cust = pd.read_sql(query, conn)
        print(avg_price_per_cust)
        avg_price_per_cust.to_csv("./output/average_order_value_per_customer.csv", index=False)

        query = """
        SELECT category, SUM(quantity * price_x) AS total_revenue
        FROM customer_transactions
        GROUP BY category;
        """
        total_rev_per_prod = pd.read_sql(query, conn)
        print(total_rev_per_prod)
        total_rev_per_prod.to_csv("./output/total_revenue_per_product.csv", index=False)

        print("Done")
