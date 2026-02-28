import requests
from web_farm.secret_store import SecretStore


def test_bearer_header(tmp_path):
    secrets_path = tmp_path / "secrets.json"
    secrets_path.write_text('{"client_api":{"type":"bearer","token":"T123"}}', encoding="utf-8")
    store = SecretStore(str(secrets_path))

    hook = store.make_auth_hook({"ref": "client_api"})

    session = requests.Session()
    headers = {}
    params = {}

    hook(session, "https://api.example.com/v1/items", params, headers)
    assert headers.get("Authorization") == "Bearer T123"


def test_cookies_file(tmp_path):
    (tmp_path / "cookies.json").write_text(
        '[{"name":"sid","value":"S1","domain":"example.com","path":"/"}]',
        encoding="utf-8",
    )
    secrets_path = tmp_path / "secrets.json"
    secrets_path.write_text('{"site_cookies":{"type":"cookies_file","path":"cookies.json"}}', encoding="utf-8")
    store = SecretStore(str(secrets_path))

    hook = store.make_auth_hook({"by_domain": {"example.com": "site_cookies"}})

    session = requests.Session()
    headers = {}
    params = {}
    hook(session, "https://example.com/page", params, headers)

    assert session.cookies.get("sid") == "S1"


def test_api_key_query(tmp_path):
    secrets_path = tmp_path / "secrets.json"
    secrets_path.write_text('{"q":{"type":"api_key_query","param":"key","token":"K9"}}', encoding="utf-8")
    store = SecretStore(str(secrets_path))

    hook = store.make_auth_hook({"ref": "q"})

    session = requests.Session()
    headers = {}
    params = {}
    hook(session, "https://example.com/api", params, headers)

    assert params.get("key") == "K9"
