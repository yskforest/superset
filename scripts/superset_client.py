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
        if res is None:
            raise RuntimeError("❌ Supersetへのログインに失敗しました")
        token = res.json().get("access_token")
        if not token:
            raise RuntimeError("❌ アクセストークンが取得できませんでした")
        self.set_auth_token(token)

        # CSRFトークン取得（重要！）
        csrf_resp = self.get("api/v1/security/csrf_token/")
        if csrf_resp is None:
            raise RuntimeError("❌ CSRFトークンの取得に失敗しました")

        # `RestApiClient`の`get`メソッド内で`raise_for_status`が呼ばれていると想定されるため、
        # ここでの呼び出しは不要です。また、キーが存在しない場合に備え.get()を使用します。
        csrf_token = csrf_resp.json().get("result")
        if not csrf_token:
            raise RuntimeError("❌ CSRFトークンがレスポンスに含まれていませんでした")

        self.session.headers.update({"X-CSRFToken": csrf_token})

    def _request(self, method: str, endpoint: str, **kwargs) -> Response | None:
        """リクエストを送信する内部メソッド。

        成功した場合はrequests.Responseオブジェクトを、失敗した場合はNoneを返します。

        Returns:
            Response | None: 成功時はResponseオブジェクト、失敗時はNone。
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            self.logger.debug(f"Request: {method} {url} with {kwargs}")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()  # 2xx 以外で HTTPError を送出
            self.logger.info(f"Success: {method} {url} -> {response.status_code}")
            return response
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP Error: {method} {url} -> {e.response.status_code}: {e.response.text}")
            return response
        except Exception as e:
            # 予期せぬエラーはスタックトレースも記録する
            self.logger.error(f"Request Error: {method} {url} -> {e}", exc_info=True)
            return None

    def get(self, endpoint: str, **kwargs) -> Response | None:
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> Response | None:
        return self._request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> Response | None:
        return self._request("PUT", endpoint, **kwargs)

    def patch(self, endpoint: str, **kwargs) -> Response | None:
        return self._request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> Response | None:
        return self._request("DELETE", endpoint, **kwargs)

    def create_dataset(self, database_id: int, table_name: str, schema: str | None = None) -> int:
        """データセットを作成し、そのIDを返します。"""
        payload = {"database": database_id, "table_name": table_name, "schema": schema}
        res = self.post("api/v1/dataset/", json=payload)
        if res is None:
            # 失敗時はNoneを返すのではなく、例外を送出して呼び出し元にエラーを通知します。
            raise RuntimeError(f"❌ データセット '{table_name}' の作成に失敗しました")
        return res.json()["id"]

    def create_chart(
        self, dataset_id: int, chart_name: str, viz_type: str = "table", params: dict | None = None
    ) -> int:
        """チャートを作成し、そのIDを返します。"""
        # デフォルト引数にミュータブルなオブジェクト(dict)を使うのは危険なため避けます。
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
        if res is None:
            raise RuntimeError(f"❌ チャート '{chart_name}' の作成に失敗しました")
        return res.json()["id"]

    def create_dashboard(self, dashboard_title: str, chart_ids: list[int]) -> int:
        """ダッシュボードを作成し、そのIDを返します。"""
        payload = {"dashboard_title": dashboard_title, "positions": {}, "charts": chart_ids}
        res = self.post("api/v1/dashboard/", json=payload)
        if res is None:
            raise RuntimeError(f"❌ ダッシュボード '{dashboard_title}' の作成に失敗しました")
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
