import argparse
import psycopg2


def execute_sql_file(host, port, user, password, dbname, sql_file_path):
    try:
        # データベースに接続
        connection = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        cursor = connection.cursor()

        # SQLファイルを読み込み、実行
        with open(sql_file_path, "r") as file:
            sql = file.read()
            cursor.execute(sql)
            connection.commit()
        print("SQLファイルの実行が成功しました。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")

    finally:
        # リソースを解放
        if cursor:
            cursor.close()
        if connection:
            connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PostgreSQLでSQLファイルを実行するスクリプト")
    parser.add_argument("-H", "--host", default="localhost", help="PostgreSQLサーバーのホスト名")
    parser.add_argument("-P", "--port", default="5432", help="PostgreSQLサーバーのポート番号")
    parser.add_argument("-U", "--user", default="superset", help="PostgreSQLのユーザー名")
    parser.add_argument("-W", "--password", default="superset", help="PostgreSQLのパスワード")
    parser.add_argument("-D", "--dbname", default="superset", help="接続するデータベース名")
    parser.add_argument("-i", "--inputsql", required=True, help="実行するSQLファイルのパス")
    args = parser.parse_args()

    # SQLファイルを実行
    execute_sql_file(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        sql_file_path=args.inputsql,
    )
