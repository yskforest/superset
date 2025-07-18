import argparse
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime


def import_csv_to_postgres(csv_file, user, password, host, port, database, table, schema, tag):
    # Check if the CSV file exists
    if not os.path.exists(csv_file):
        print(f"CSV file '{csv_file}' does not exist. Skipping import.")
        return

    # Read the CSV file
    try:
        df = pd.read_csv(csv_file)
        # Add a timestamp column with the current time
        df["timestamp"] = datetime.now()
        # Add a tag column with the specified tag
        df["tag"] = tag
    except Exception as e:
        print(f"Error occurred while reading the CSV file: {e}")
        return

    # Connect to PostgreSQL
    try:
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        # Import data into PostgreSQL
        df.to_sql(table, con=engine, schema=schema, if_exists="append", index=False)
        print(f"Data successfully imported into table '{schema}.{table}'.")
    except Exception as e:
        print(f"Error occurred during PostgreSQL connection or data import: {e}")


def main():
    # Define command-line arguments using argparse
    parser = argparse.ArgumentParser(description="Script to import a CSV file into PostgreSQL")
    parser.add_argument("csv_file", type=str, help="Path to the CSV file to be imported")
    parser.add_argument("-u", "--user", type=str, default="superset", help="PostgreSQL username (default: superset)")
    parser.add_argument(
        "-p", "--password", type=str, default="superset", help="PostgreSQL password (default: superset)"
    )
    parser.add_argument("-H", "--host", type=str, default="localhost", help="PostgreSQL host (default: localhost)")
    parser.add_argument("-P", "--port", type=int, default=5432, help="PostgreSQL port (default: 5432)")
    parser.add_argument(
        "-d", "--database", type=str, default="superset", help="PostgreSQL database name (default: superset)"
    )
    parser.add_argument(
        "-t",
        "--table",
        type=str,
        default="cloc_by_file_table",
        help="Target table name for import (default: cloc_by_file_table)",
    )
    parser.add_argument("-s", "--schema", type=str, default="public", help="PostgreSQL schema name (default: public)")
    parser.add_argument("--tag", type=str, default="default", help="Tag to add to the imported data (default: default)")

    args = parser.parse_args()

    print("Starting CSV import...")
    import_csv_to_postgres(
        csv_file=args.csv_file,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        database=args.database,
        table=args.table,
        schema=args.schema,
        tag=args.tag,
    )
    print("CSV import completed.")


if __name__ == "__main__":
    main()
