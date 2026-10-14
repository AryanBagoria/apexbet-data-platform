"""Storage backend tests (local backend + interface)."""

from __future__ import annotations

from apexbet.common.storage import parquet_bytes, read_parquet_bytes


class TestLocalStore:
    def test_put_get_exists(self, tmp_store):
        tmp_store.put_bytes("a/b/c.txt", b"hello")
        assert tmp_store.exists("a/b/c.txt")
        assert tmp_store.get_bytes("a/b/c.txt") == b"hello"

    def test_list_prefix(self, tmp_store):
        tmp_store.put_bytes("bronze/event_type=bet_placed/f1.parquet", b"1")
        tmp_store.put_bytes("bronze/event_type=bet_placed/f2.parquet", b"2")
        tmp_store.put_bytes("silver/f1.parquet", b"3")
        assert len(tmp_store.list("bronze")) == 2
        assert len(tmp_store.list("bronze/event_type=bet_placed")) == 2
        assert len(tmp_store.list("silver")) == 1

    def test_list_nonliteral_prefix(self, tmp_store):
        # "bronze/event_type=" is not a real directory; must still match.
        tmp_store.put_bytes("bronze/event_type=bet_placed/f1.parquet", b"1")
        tmp_store.put_bytes("bronze/event_type=odds_updated/f2.parquet", b"2")
        assert len(tmp_store.list("bronze/event_type=")) == 2

    def test_delete(self, tmp_store):
        tmp_store.put_bytes("x", b"1")
        tmp_store.delete("x")
        assert not tmp_store.exists("x")

    def test_path_traversal_blocked(self, tmp_store):
        import pytest

        with pytest.raises(ValueError):
            tmp_store.put_bytes("../escape.txt", b"x")

    def test_health(self, tmp_store):
        assert tmp_store.health()["status"] == "OK"

    def test_parquet_roundtrip(self):
        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        data = parquet_bytes(rows)
        assert read_parquet_bytes(data) == rows
