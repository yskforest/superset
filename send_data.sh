#!/bin/bash

# 必要な情報を設定
CSV_FILE="cloc_result.csv" # インポートするCSVファイル
DB_HOST="localhost"    # SupersetのPostgreSQLサーバーのIPアドレス
DB_PORT="5432"             # PostgreSQLのポート番号
DB_USER="superset"         # PostgreSQLユーザー
DB_PASSWORD="superset"     # PostgreSQLパスワード
DB_NAME="superset"         # データベース名

# テーブル作成クエリ
CREATE_TABLE_SQL=$(
    cat <<EOF
CREATE TABLE IF NOT EXISTS count_line_cloc (
    files INTEGER,
    language TEXT,
    blank INTEGER,
    comment INTEGER,
    code INTEGER
);
EOF
)

# PostgreSQLクライアントでテーブルを作成
echo "Creating table on remote PostgreSQL server..."
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$CREATE_TABLE_SQL"

# CSVデータをPostgreSQLにコピー
echo "Importing CSV data into PostgreSQL..."
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\
COPY count_line_cloc (files, language, blank, comment, code) \
FROM STDIN WITH (FORMAT csv, HEADER true);" <$CSV_FILE

echo "Data import completed successfully!"
