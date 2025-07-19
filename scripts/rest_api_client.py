import logging
from typing import Any

import requests


class RestApiClient:
    def __init__(self, base_url: str, headers: dict[str, str] | None = None, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.timeout = timeout

        # ロギング設定
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        self.logger = logging.getLogger(__name__)

    def set_auth_token(self, token: str):
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _log(self, message: str, level: str = "info"):
        log_levels = {
            "info": self.logger.info,
            "warning": self.logger.warning,
            "error": self.logger.error,
            "debug": self.logger.debug,
        }
        log_levels.get(level, self.logger.info)(message)

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        kwargs.setdefault("timeout", self.timeout)

        try:
            self._log(f"{method} request to {url} with {kwargs}", "debug")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()

            # JSONレスポンスの処理
            if "application/json" in response.headers.get("Content-Type", ""):
                try:
                    json_data = response.json()
                    self._log(f"Response: {response.status_code} (JSON)", "debug")
                    return json_data
                except ValueError:
                    self._log(f"Failed to parse JSON response from {url}", "warning")
                    return None

            # JSON以外のレスポンス
            self._log(f"Response: {response.status_code} (Text)", "debug")
            return {"status_code": response.status_code, "content": response.text}

        except requests.exceptions.Timeout:
            self._log(f"Timeout Error: {method} {url}", "error")
            return None
        except requests.exceptions.HTTPError as e:
            self._log(f"HTTP Error: {method} {url} → {e.response.status_code}: {e.response.text}", "error")
            return None
        except requests.exceptions.RequestException as e:
            self._log(f"Request Error: {method} {url} → {e}", "error")
            return None
        except Exception as e:
            self._log(f"Unexpected Error: {method} {url} → {e}", "error")
            raise

    def get(self, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        return self._request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        return self._request("PUT", endpoint, **kwargs)

    def patch(self, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        return self._request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> dict[str, Any] | list[Any] | None:
        return self._request("DELETE", endpoint, **kwargs)

    def close(self):
        self.session.close()
