import argparse
import json
import logging
import sys

import requests
from requests import Response

# ライブラリとして利用されることを想定し、ロギング設定は呼び出し元に委ねる。
# 呼び出し元がロギング設定しない場合に警告が出るのを防ぐため、NullHandlerを追加。
logging.getLogger(__name__).addHandler(logging.NullHandler())


class RestApiClient:
    """汎用的なREST APIクライアント。"""

    def __init__(self, base_url: str, headers: dict[str, str] | None = None, timeout: int = 10):
        """
        Args:
            base_url (str): APIのベースURL。
            headers (dict[str, str] | None, optional): デフォルトのリクエストヘッダー。 Defaults to None.
            timeout (int, optional): リクエストのタイムアウト秒数。 Defaults to 10.
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def set_auth_token(self, token: str, token_type: str = "Bearer"):
        """認証トークンをセッションヘッダーに設定します。"""
        self.session.headers.update({"Authorization": f"{token_type} {token}"})

    def _request(self, method: str, endpoint: str, **kwargs) -> Response | None:
        """リクエストを送信する内部メソッド。

        成功した場合はrequests.Responseオブジェクトを、失敗した場合はNoneを返します。

        Returns:
            Response | None: 成功時はResponseオブジェクト、失敗時はNone。
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        kwargs.setdefault("timeout", self.timeout)

        try:
            self.logger.debug(f"Request: {method} {url} with {kwargs}")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()  # 2xx 以外で HTTPError を送出
            self.logger.info(f"Success: {method} {url} -> {response.status_code}")
            return response
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout Error: {method} {url}")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP Error: {method} {url} -> {e.response.status_code}: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request Error: {method} {url} -> {e}")
            return None
        except Exception as e:
            # 予期せぬエラーはスタックトレースも記録する
            self.logger.error(f"Unexpected Error: {method} {url} -> {e}", exc_info=True)
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

    def close(self):
        """セッションを明示的に閉じます。"""
        if self.session:
            self.logger.info("Closing session.")
            self.session.close()


def _parse_key_value_arg(arg_list: list[str] | None, delimiter: str) -> dict:
    """
    'key:value' や 'key=value' 形式の引数リストを辞書に変換します。

    Args:
        arg_list (list[str] | None): argparseからの引数リスト。
        delimiter (str): キーと値を区切る文字。

    Returns:
        dict: パースされたキーと値の辞書。
    """
    result = {}
    if not arg_list:
        return result
    for item in arg_list:
        try:
            key, value = item.split(delimiter, 1)
            result[key.strip()] = value.strip()
        except ValueError:
            logging.error(f"不正な形式です: '{item}'。'{delimiter}'で区切ってください。")
            sys.exit(1)
    return result


def _prepare_request_body(data_arg: str | None) -> dict:
    """--data引数からリクエストボディ用の辞書を準備します。"""
    if not data_arg:
        return {}

    if data_arg.startswith("@"):
        filepath = data_arg[1:]
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return {"json": json.load(f)}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"データファイル '{filepath}' の読み込みまたはパースに失敗しました: {e}")
            sys.exit(1)
    else:
        try:
            return {"json": json.loads(data_arg)}
        except json.JSONDecodeError:
            return {"data": data_arg}  # JSONでなければプレーンテキストとして扱う


def _print_response(response: Response):
    """レスポンスを整形して標準出力に表示します。"""
    print(f"Status Code: {response.status_code}\n")
    print("--- Headers ---")
    for key, value in response.headers.items():
        print(f"{key}: {value}")
    print("\n--- Body ---")
    try:
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(response.text)


def main():
    """コマンドラインから簡易的にREST APIをテストするためのメイン関数。"""
    parser = argparse.ArgumentParser(
        description="コマンドラインから利用できるシンプルなREST APIクライアント",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""実行例:
  python %(prog)s GET https://api.github.com /users/google -v
  python %(prog)s POST https://httpbin.org /post -d '{"key":"value"}' -H "Content-Type: application/json"
  python %(prog)s POST https://httpbin.org /post -d @data.json
""",
    )
    parser.add_argument(
        "method", choices=["GET", "POST", "PUT", "PATCH", "DELETE"], help="HTTPメソッド (GET, POSTなど)"
    )
    parser.add_argument("base_url", help="APIのベースURL (例: https://api.github.com)")
    parser.add_argument("endpoint", help="APIのエンドポイント (例: /users/google)")
    parser.add_argument(
        "-d",
        "--data",
        help="リクエストボディ。JSON文字列または '@' で始まるファイルパス (例: '{\"key\":\"value\"}' or @data.json)",
    )
    parser.add_argument(
        "-H", "--header", action="append", help="リクエストヘッダー (例: -H 'Content-Type: application/json')"
    )
    parser.add_argument("-p", "--params", action="append", help="クエリパラメータ (例: -p 'key1=value1')")
    parser.add_argument("-t", "--timeout", type=int, help="リクエストのタイムアウト秒数 (デフォルト: 10)")
    parser.add_argument("-v", "--verbose", action="store_true", help="詳細なログを出力する")

    args = parser.parse_args()

    # ロギング設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = "%(asctime)s - %(levelname)s - %(message)s" if args.verbose else "%(message)s"
    logging.basicConfig(level=log_level, format=log_format, stream=sys.stderr)

    # 引数からリクエストパラメータを準備
    headers = _parse_key_value_arg(args.header, ":")
    params = _parse_key_value_arg(args.params, "=")
    request_body_kwargs = _prepare_request_body(args.data)

    # クライアントの初期化とリクエスト実行
    client_kwargs = {"headers": headers}
    if args.timeout:
        client_kwargs["timeout"] = args.timeout

    request_kwargs = {"params": params, **request_body_kwargs}

    with RestApiClient(args.base_url, **client_kwargs) as client:
        method_to_call = getattr(client, args.method.lower())
        response = method_to_call(args.endpoint, **request_kwargs)

        if response:
            _print_response(response)
        else:
            logging.error("リクエストに失敗しました。詳細はログを確認してください。")
            sys.exit(1)


if __name__ == "__main__":
    main()
