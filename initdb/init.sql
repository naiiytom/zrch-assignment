CREATE SCHEMA raw;
CREATE SCHEMA ingest;
CREATE SCHEMA curate;

CREATE TABLE raw.product_catalog (
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    price VARCHAR
);

CREATE TABLE raw.customer_transactions (
    transaction_id VARCHAR,
    customer_id VARCHAR,
    product_id VARCHAR,
    quantity VARCHAR,
    price VARCHAR,
    timestamp VARCHAR
);

CREATE TABLE ingest.product_catalog (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL
);

CREATE TABLE ingest.customer_transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    product_id VARCHAR,
    quantity INT,
    price DECIMAL,
    timestamp TIMESTAMP
);

CREATE TABLE curate.unified_transactions (
    transaction_id VARCHAR PRIMARY KEY, 
    customer_id VARCHAR, 
    product_id VARCHAR, 
    quantity INT, 
    price_x DECIMAL, 
    timestamp TIMESTAMP, 
    product_name VARCHAR, 
    category VARCHAR, 
    price_y DECIMAL
);