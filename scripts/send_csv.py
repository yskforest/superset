import psycopg2
import argparse


def import_csv_to_postgres(host, port, user, password, dbname, table_name, csv_file):
    """
    PostgreSQLのテーブルにCSVデータをインポートする
    """
    try:
        # PostgreSQLデータベースに接続
        print("Connecting to PostgreSQL database...")
        connection = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)

        cursor = connection.cursor()

        # COPYコマンドでCSVファイルをインポート
        print(f"Importing data from {csv_file} into table {table_name}...")
        with open(csv_file, "r") as f:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            # f"COPY {table_name} (Kind, Name, File, CountLine, CountLineCode,
            # CountLineComment, MaxCyclomatic, MaxNesting) FROM STDIN WITH (FORMAT
            # csv, HEADER true)", f)

            # f"COPY {table_name} (language, filename, blank, comment, code) FROM
            # STDIN WITH (FORMAT csv, HEADER true)", f)

        # コミットしてデータベースに反映
        connection.commit()
        print("CSV data imported successfully!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # 接続を閉じる
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import CSV data into a PostgreSQL table")
    parser.add_argument("--host", default="localhost", help="PostgreSQL server host")
    parser.add_argument("--port", default="5432", help="PostgreSQL server port")
    parser.add_argument("--user", default="superset", help="PostgreSQL user")
    parser.add_argument("--password", default="superset", help="PostgreSQL password")
    parser.add_argument("--dbname", default="superset", help="PostgreSQL database name")
    parser.add_argument("-t", "--table", default="und_file_table", help="Target table name")
    parser.add_argument("-i", "--input_csv", required=True, help="Path to the CSV file")
    args = parser.parse_args()

    import_csv_to_postgres(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        table_name=args.table,
        csv_file=args.input_csv,
    )
