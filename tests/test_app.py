import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_home():
    res = client.get('/')
    assert res.status_code == 200
    assert '리니지클래식 통합 포털' in res.text


def test_search_api():
    res = client.get('/api/search?q=검')
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_spells_page():
    res = client.get('/spells')
    assert res.status_code == 200
    assert '마법 DB' in res.text


def test_skins_page():
    res = client.get('/skins')
    assert res.status_code == 200
    assert '스킨 / 혈마크 자료실' in res.text
