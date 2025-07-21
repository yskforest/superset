import argparse
import os
from datetime import datetime

import pandas as pd
import psycopg
from psycopg import sql
from sqlalchemy import create_engine


class PostgresClient:
    """PostgreSQLデータベースを操作するためのクライアントクラス。"""

    def __init__(self, user, password, host, port, dbname):
        self.connection = psycopg.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")
        self.engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャの終了時にデータベース接続を閉じます。"""
        if self.connection:
            self.connection.close()

    def create_schema(self, schema_name):
        with self.connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
            self.connection.commit()

    def import_csv_to_postgres(self, csv_file, table, schema, timestamp=True, tag=None):
        """CSVファイルを読み込み、PostgreSQLのテーブルにインポートします。"""
        if not os.path.exists(csv_file):
            print(f"CSV file '{csv_file}' does not exist. Skipping import.")
            return

        try:
            df = pd.read_csv(csv_file)
            if timestamp:
                df["timestamp"] = datetime.now()
            if tag:
                df["tag"] = tag
        except Exception as e:
            print(f"Error occurred while reading the CSV file: {e}")
            return

        try:
            df.to_sql(table, con=self.engine, schema=schema, if_exists="append", index=False)
            print(f"Data successfully imported into table '{schema}.{table}'.")
        except Exception as e:
            print(f"Error occurred during data import: {e}")


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
    try:
        with PostgresClient(
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            database=args.database,
        ) as client:
            client.create_schema(args.schema)
            client.import_csv_to_postgres(
                csv_file=args.csv_file,
                table=args.table,
                schema=args.schema,
                tag=args.tag,
            )
    except Exception as e:
        print(f"An error occurred: {e}")

    print("CSV import completed.")


if __name__ == "__main__":
    main()
