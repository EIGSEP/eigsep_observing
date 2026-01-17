"""Global pytest configuration for all tests."""
import pytest
import fakeredis


@pytest.fixture(autouse=True)
def mock_redis(monkeypatch):
    """
    Globally mock redis.Redis with fakeredis.FakeRedis for all tests.

    This ensures that any code that creates a redis.Redis instance
    (directly or indirectly) will use fakeredis instead, preventing
    attempts to connect to a real Redis server at localhost:6379.
    """
    monkeypatch.setattr("redis.Redis", fakeredis.FakeRedis)
