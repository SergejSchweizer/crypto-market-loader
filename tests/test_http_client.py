"""Tests for HTTP client retry behavior."""

from __future__ import annotations

from email.message import Message
from urllib.error import HTTPError, URLError

import pytest

from ingestion import http_client
from ingestion.http_client import HttpClientError, HttpClientHttpError, get_json


class _FakeResponse:
    """Context-manager response stub used for urlopen monkeypatching."""

    def __init__(self, payload: str) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload.encode("utf-8")

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


def test_get_json_retries_on_url_error_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        calls["count"] += 1
        if calls["count"] == 1:
            raise URLError("timed out")
        return _FakeResponse('{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    payload = get_json("https://example.com", max_retries=2, retry_backoff_s=0.0)
    assert payload == {"ok": True}
    assert calls["count"] == 2


def test_get_json_retries_on_http_500_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(url=url, code=500, msg="server error", hdrs=Message(), fp=None)
        return _FakeResponse('{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    payload = get_json("https://example.com", max_retries=2, retry_backoff_s=0.0)
    assert payload == {"ok": True}
    assert calls["count"] == 2


def test_get_json_does_not_retry_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        calls["count"] += 1
        raise HTTPError(url=url, code=400, msg="bad request", hdrs=Message(), fp=None)

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    with pytest.raises(HttpClientError, match="HTTP error 400"):
        get_json("https://example.com", max_retries=3, retry_backoff_s=0.0)
    assert calls["count"] == 1


def test_get_json_raises_typed_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        raise HTTPError(url=url, code=503, msg="unavailable", hdrs=Message(), fp=None)

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    with pytest.raises(HttpClientHttpError) as exc_info:
        get_json("https://example.com", max_retries=0, retry_backoff_s=0.0)

    assert exc_info.value.status_code == 503
    assert exc_info.value.retryable is True


def test_http_env_parsers_and_retryable_classifier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X_FLOAT", "bad")
    assert http_client._env_float("X_FLOAT", 1.5) == 1.5
    monkeypatch.setenv("X_FLOAT", "-1")
    assert http_client._env_float("X_FLOAT", 1.5) == 1.5
    monkeypatch.setenv("X_FLOAT", "2.5")
    assert http_client._env_float("X_FLOAT", 1.5) == 2.5

    monkeypatch.setenv("X_INT", "bad")
    assert http_client._env_int("X_INT", 2) == 2
    monkeypatch.setenv("X_INT", "-1")
    assert http_client._env_int("X_INT", 2) == 2
    monkeypatch.setenv("X_INT", "3")
    assert http_client._env_int("X_INT", 2) == 3

    err_429 = HTTPError(url="https://x", code=429, msg="x", hdrs=Message(), fp=None)
    err_500 = HTTPError(url="https://x", code=500, msg="x", hdrs=Message(), fp=None)
    err_400 = HTTPError(url="https://x", code=400, msg="x", hdrs=Message(), fp=None)
    assert http_client._is_retryable_http_error(err_429) is True
    assert http_client._is_retryable_http_error(err_500) is True
    assert http_client._is_retryable_http_error(err_400) is False


def test_get_json_timeout_and_invalid_json_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _timeout(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        raise TimeoutError("boom")

    monkeypatch.setattr(http_client, "urlopen", _timeout)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)
    with pytest.raises(HttpClientError, match="Connection timeout"):
        get_json("https://example.com", max_retries=0)

    monkeypatch.setattr(http_client, "urlopen", lambda url, timeout: _FakeResponse("not-json"))
    with pytest.raises(HttpClientError, match="Invalid JSON"):
        get_json("https://example.com", max_retries=0)


def test_get_json_http_400_sets_retryable_false(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        raise HTTPError(url=url, code=400, msg="bad request", hdrs=Message(), fp=None)

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    with pytest.raises(HttpClientHttpError) as exc_info:
        get_json("https://example.com", max_retries=0)
    assert exc_info.value.status_code == 400
    assert exc_info.value.retryable is False
