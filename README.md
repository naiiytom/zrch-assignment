# Data Engineering Assignment By ZRCH

## Prerequisites

- Linux host
- Docker with Docker Compose

## Setup Instructions

1. **Clone the repository:**

    ```bash
    git clone https://github.com/naiiytom/zrch-assignment.git
    cd zrch-assignment
    ```

2. **Prepare Raw Data Files:**

    Place your raw data files in a directory named `raw`:
    - `customer_transactions.json` (JSON file)
    - `product_catalog.csv` (CSV file)

3. **To Run the ETL Pipeline:**

    ```bash
    sh start.sh
    ```

4. **To Connect to the PostgreSQL database**

    Please connect on port `5432` to run query on database with GUI application

5. **To Wrap Up**

    ```bash
    sh stop.sh
    ```

6. **Results**

    The result csv files are placed in `output` directory

7. **Answer**

    The answer is to be found under [docs](./docs/) directory
