import main
import pytest


@pytest.fixture
def client():
    main.APP.testing = True
    return main.APP.test_client()


def test_handler_home(client):
    r = client.get("/")
    assert r.status_code == 404


def test_handler_migrate_data(client):
    r = client.get("/migrate-data")
    assert r.status_code == 405

    r = client.post("/migrate-data")
    assert r.status_code == 400
