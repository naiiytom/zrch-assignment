import os
import pandas as pd
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Environment variable containing the database connection string
DB_CONNECTION = os.environ["DB_CONNECTION"]


def load_data(path: str, type: str) -> pd.DataFrame:
    """
    Load data from a given file path based on the file type.

    Args:
    - path (str): The file path to load data from.
    - type (str): The type of the file ('csv' or 'json').

    Returns:
    - pd.DataFrame: Loaded data as a pandas DataFrame.

    Raises:
    - Exception: If the file extension is not supported.
    """
    if type == "csv":
        return pd.read_csv(path)
    elif type == "json":
        return pd.read_json(path)
    else:
        raise Exception("File extension not supported")


def is_valid_value(value) -> bool:
    """
    Check if a value is a valid positive number.

    Args:
    - value (any): The value to check.

    Returns:
    - bool: True if the value is a valid positive number, False otherwise.
    """
    try:
        val = float(value)
        return val > 0
    except ValueError:
        return False


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the given DataFrame by removing duplicates, converting timestamps,
    and filtering out invalid price values.

    Args:
    - df (pd.DataFrame): The DataFrame to clean.

    Returns:
    - pd.DataFrame: The cleaned DataFrame.
    """
    df = df.drop_duplicates()  # Remove duplicate rows
    col_names = df.columns
    if "timestamp" in col_names:
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Convert to datetime
    if "price" in col_names:
        df = df[df["price"].apply(is_valid_value)]  # Filter out invalid prices
    return df


if __name__ == "__main__":
    # Attempt to connect to the PostgreSQL database
    while True:
        try:
            engine = create_engine(DB_CONNECTION)  # Create database engine
            conn = engine.connect()  # Attempt to connect
            conn.close()  # Close the connection if successful
            break
        except OperationalError as e:
            time.sleep(5)  # Wait for 5 seconds before retrying

    # Establish a new connection within the context
    with engine.connect() as conn:
        # Load raw data files
        customer_txn_df = load_data("./raw/customer_transactions.json", "json")
        product_cat_df = load_data("./raw/product_catalog.csv", "csv")

        # Write raw data into the 'raw' schema in the database
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

        # Clean the loaded data
        clean_customer_txn_df = clean_data(customer_txn_df)
        clean_product_cat_df = clean_data(product_cat_df)

        # Write cleaned data into the 'ingest' schema in the database
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

        # Merge the cleaned customer transactions and product catalog data
        merge_data = clean_customer_txn_df.merge(
            clean_product_cat_df, on="product_id", how="inner"
        )
        # Write the merged data into the 'curate' schema in the database
        merge_data.to_sql(
            "unified_transactions",
            schema="curate",
            con=conn,
            if_exists="append",
            index=False,
        )

        # Query to find the top 2 best-selling products
        query = """
        SELECT product_id, product_name, SUM(quantity) AS total_sold
        FROM curate.unified_transactions
        GROUP BY product_id, product_name
        ORDER BY total_sold DESC
        LIMIT 2;
        """
        top_2_selling = pd.read_sql(query, conn)  # Execute the query
        print(top_2_selling)
        top_2_selling.to_csv(
            "./output/top2_best_selling.csv", index=False
        )  # Save to CSV

        # Query to find the average order value per customer
        query = """
        SELECT customer_id, AVG(quantity * price_x) AS average_order_value
        FROM curate.unified_transactions
        GROUP BY customer_id;
        """
        avg_price_per_cust = pd.read_sql(query, conn)  # Execute the query
        print(avg_price_per_cust)
        avg_price_per_cust.to_csv(
            "./output/average_order_value_per_customer.csv", index=False
        )  # Save to CSV

        # Query to find the total revenue generated per product category
        query = """
        SELECT category, SUM(quantity * price_x) AS total_revenue
        FROM curate.unified_transactions
        GROUP BY category;
        """
        total_rev_per_prod = pd.read_sql(query, conn)  # Execute the query
        print(total_rev_per_prod)
        total_rev_per_prod.to_csv(
            "./output/total_revenue_per_product.csv", index=False
        )  # Save to CSV

        print("Done")  # Indicate completion
