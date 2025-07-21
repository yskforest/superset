import argparse
import json
import logging
import sys
from urllib.parse import quote

import requests
from requests import Response

# ライブラリとして利用されることを想定し、ロギング設定は呼び出し元に委ねる。
# 呼び出し元がロギング設定しない場合に警告が出るのを防ぐため、NullHandlerを追加。
logging.getLogger(__name__).addHandler(logging.NullHandler())


class SupersetClient:
    """SupersetのREST APIと連携するためのクライアント。"""

    def __init__(self, base_url: str, username: str, password: str, provider: str = "db", timeout: int = 10):
        """
        Args:
            base_url (str): SupersetのベースURL。
            username (str): ログインユーザー名。
            password (str): パスワード。
            provider (str, optional): 認証プロバイダ。通常は "db"。
        """

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.username = username
        self.password = password
        self.provider = provider
        self._authenticate()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        """セッションを明示的に閉じます。"""
        if self.session:
            self.logger.info("Closing session.")
            self.session.close()

    def _authenticate(self):
        """ログインしてアクセストークンを取得し、ヘッダーに設定します。"""
        payload = {"username": self.username, "password": self.password, "provider": self.provider, "refresh": True}
        res = self.post("api/v1/security/login", json=payload)
        res.raise_for_status()
        token = res.json().get("access_token")
        if not token:
            raise RuntimeError("❌ アクセストークンが取得できませんでした")
        self.set_auth_token(token)

        res = self.get("api/v1/security/csrf_token/")
        res.raise_for_status()

        csrf_token = res.json().get("result")
        if not csrf_token:
            raise RuntimeError("❌ CSRFトークンがレスポンスに含まれていませんでした")

        self.session.headers.update({"X-CSRFToken": csrf_token})

    def set_auth_token(self, token: str, token_type: str = "Bearer"):
        """認証トークンをセッションヘッダーに設定します。"""
        self.session.headers.update({"Authorization": f"{token_type} {token}"})

    def _request(self, method: str, endpoint: str, **kwargs) -> Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        self.logger.debug(f"Request: {method} {url} with {kwargs}")
        response = self.session.request(method, url, **kwargs)
        # response.raise_for_status()  # 2xx 以外で HTTPError を送出
        self.logger.info(f"Success: {method} {url} -> {response.text}")
        return response

    def get(self, endpoint: str, **kwargs) -> Response:
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> Response:
        return self._request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> Response:
        return self._request("PUT", endpoint, **kwargs)

    def patch(self, endpoint: str, **kwargs) -> Response:
        return self._request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> Response:
        return self._request("DELETE", endpoint, **kwargs)

    def create_dataset(self, database_id: int, table_name: str, schema: str = "public") -> int:
        """データセットを作成し、そのIDを返します。"""
        payload = {"database": database_id, "table_name": table_name, "schema": schema}
        res = self.post("api/v1/dataset/", json=payload)
        if res.status_code == 201:
            chart_id = res.json()["id"]
        elif res.status_code == 422:
            self.logger.info(f"既にデータセットが存在するので、既存の{table_name}のIDを返します。{res.text}")
            chart_id = self.get_dataset_id_by_name(table_name)
        else:
            self.logger.error(f"Request Error: POST api/v1/dataset/ -> {res}", exc_info=True)
            raise RuntimeError(f"❌ データセット '{table_name}' の作成に失敗しました: {res.text}")

        return chart_id

    def create_chart(
        self, dataset_id: int, chart_name: str, viz_type: str = "table", params: dict | None = None
    ) -> int:
        """チャートを作成し、そのIDを返します。"""
        if params is None:
            params = {}
        payload = {
            "slice_name": chart_name,
            "viz_type": viz_type,
            "params": json.dumps(params),
            "datasource_id": dataset_id,
            "datasource_type": "table",
        }
        res = self.post("api/v1/chart/", json=payload)
        res.raise_for_status()  # 2xx 以外で HTTPError を送出
        return res.json()["id"]

    def create_dashboard(self, dashboard_title: str, chart_ids: list[int]) -> int:
        """ダッシュボードを作成し、そのIDを返します。"""
        payload = {"dashboard_title": dashboard_title, "positions": {}, "charts": chart_ids}
        res = self.post("api/v1/dashboard/", json=payload)
        res.raise_for_status()  # 2xx 以外で HTTPError を送出
        return res.json()["id"]

    def get_database_id_by_name(self, db_name: str) -> int:
        """データベース名からIDを効率的に取得します。"""
        # Superset APIのフィルタリング機能(Rison形式)を利用して、
        # 全件取得ではなく、必要なデータベースのみを問い合わせます。
        rison_filter = f"(filters:!((col:database_name,opr:eq,value:'{db_name}')))"
        query = quote(rison_filter)
        res = self.get(f"api/v1/database/?q={query}")

        if res is None:
            raise RuntimeError(f"❌ データベース '{db_name}' の検索に失敗しました")

        result = res.json().get("result", [])
        if not result:
            raise ValueError(f"❌ データベース '{db_name}' が見つかりませんでした")
        if len(result) > 1:
            # 基本的にdatabase_nameはユニークですが、念のためチェックします。
            raise ValueError(f"❌ データベース名 '{db_name}' が一意に定まりませんでした")

        return result[0]["id"]

    def get_dataset_id_by_name(self, dataset_name: str) -> int:
        """データセット名からIDを効率的に取得します。

        Args:
            dataset_name (str): 検索するデータセット名 (API上ではtable_name)。

        Raises:
            RuntimeError: API呼び出しに失敗した場合。
            ValueError: データセットが見つからない、または一意に定まらない場合。

        Returns:
            int: 見つかったデータセットのID。
        """
        # Superset APIのフィルタリング機能(Rison形式)を利用して、
        # 全件取得ではなく、必要なデータセットのみを問い合わせます。
        # データセット名は 'table_name' カラムでフィルタリングします。
        rison_filter = f"(filters:!((col:table_name,opr:eq,value:'{dataset_name}')))"
        query = quote(rison_filter)
        res = self.get(f"api/v1/dataset/?q={query}")

        if res is None:
            raise RuntimeError(f"❌ データセット '{dataset_name}' の検索に失敗しました")

        result = res.json().get("result", [])
        if not result:
            raise ValueError(f"❌ データセット '{dataset_name}' が見つかりませんでした")
        if len(result) > 1:
            ids = [item.get("id") for item in result]
            raise ValueError(
                f"❌ データセット名 '{dataset_name}' が一意に定まりませんでした。複数のIDが見つかりました: {ids}"
            )

        return result[0]["id"]
