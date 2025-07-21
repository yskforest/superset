import json

from rest_api_client import RestApiClient


class SupersetClient(RestApiClient):
    """SupersetのREST APIと連携するためのクライアント。"""

    def __init__(self, base_url: str, username: str, password: str, provider: str = "db"):
        """
        Args:
            base_url (str): SupersetのベースURL。
            username (str): ログインユーザー名。
            password (str): パスワード。
            provider (str, optional): 認証プロバイダ。通常は "db"。
        """
        super().__init__(base_url)
        self.username = username
        self.password = password
        self.provider = provider
        self._authenticate()

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
        csrf_resp.raise_for_status()
        csrf_token = csrf_resp.json()["result"]
        self.session.headers.update({"X-CSRFToken": csrf_token})

    def create_dataset(self, database_id: int, table_name: str, schema: str = None) -> int | None:
        """データセットを作成し、そのIDを返します。"""
        payload = {"database": database_id, "table_name": table_name, "schema": schema}
        res = self.post("api/v1/dataset/", json=payload)
        if res is None:
            print("❌ データセットの作成に失敗しました")
            return None
        else:
            return res.json()["id"]

    def create_chart(self, dataset_id: int, chart_name: str, viz_type: str = "table", params: dict = {}) -> int:
        """チャートを作成し、そのIDを返します。"""
        payload = {
            "slice_name": chart_name,
            "viz_type": viz_type,
            "params": json.dumps(params),
            "datasource_id": dataset_id,
            "datasource_type": "table",
        }
        res = self.post("api/v1/chart/", json=payload)
        if res is None:
            print("❌ チャートの作成に失敗しました")
            return None
        return res.json()["id"]

    def create_dashboard(self, dashboard_title: str, chart_ids: list[int]) -> int:
        """ダッシュボードを作成し、そのIDを返します。"""
        payload = {"dashboard_title": dashboard_title, "positions": {}, "charts": chart_ids}
        res = self.post("api/v1/dashboard/", json=payload)
        if res is None:
            raise RuntimeError("❌ ダッシュボードの作成に失敗しました")
        return res.json()["id"]

    def get_database_id_by_name(self, db_name: str) -> int:
        """データベース名からIDを取得します。"""
        res = self.get("api/v1/database/")
        if res is None:
            raise RuntimeError("❌ データベース一覧の取得に失敗しました")

        databases = res.json().get("result", [])
        for db in databases:
            if db["database_name"] == db_name:
                return db["id"]

        raise ValueError(f"❌ Database '{db_name}' が見つかりませんでした")
