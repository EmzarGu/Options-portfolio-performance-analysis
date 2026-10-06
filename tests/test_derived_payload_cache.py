"""Readers must see a complete old or new generation, including during failures."""
from copy import deepcopy
from types import SimpleNamespace

import pytest
from portfolio_backend import derived_payload_cache as cache


class Store:
    def __init__(self):
        self.docs = {}
        self.on_write = lambda path, value: None

    def collection(self, name):
        return Ref(self, name)


class Ref:
    def __init__(self, store, path):
        self.store, self.path = store, path

    def collection(self, name):
        return Ref(self.store, self.path + "/" + name)

    document = collection

    def get(self):
        value = deepcopy(self.store.docs.get(self.path))
        return SimpleNamespace(exists=value is not None, to_dict=lambda: value)

    def set(self, value, **kwargs):
        self.store.on_write(self.path, value)
        self.store.docs[self.path] = deepcopy(value)


@pytest.fixture
def store(monkeypatch):
    store = Store()
    monkeypatch.setattr(cache, "firestore_client", lambda: store)
    monkeypatch.setattr(cache, "CHUNK_SIZE", 12)
    return store


def test_readers_keep_old_payload_until_all_chunks_are_written(store):
    old, new = {"values": list(range(60))}, {"values": list(range(120, 200))}
    cache.save_derived_payload("key", old)
    seen = []
    store.on_write = lambda path, value: seen.append(cache.load_derived_payload("key"))
    cache.save_derived_payload("key", new)
    assert len(seen) > 2
    assert all(value == old for value in seen)
    assert cache.load_derived_payload("key") == new


def test_interrupted_write_preserves_previous_generation(store):
    cache.save_derived_payload("key", {"old": True})
    writes = 0
    def fail(path, value):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("interrupted")
    store.on_write = fail
    cache.save_derived_payload("key", {"large": list(range(100))})
    store.on_write = lambda *args: None
    assert cache.load_derived_payload("key") == {"old": True}


def test_concurrent_writer_cannot_mix_chunks(store):
    first, second = {"first": list(range(100))}, {"second": list(range(50))}
    fired = False
    def interleave(path, value):
        nonlocal fired
        if not fired:
            fired = True
            cache.save_derived_payload("key", second)
    store.on_write = interleave
    cache.save_derived_payload("key", first)
    assert cache.load_derived_payload("key") == first


def test_corrupt_chunks_and_old_schema_are_cache_misses(store):
    cache.save_derived_payload("key", {"ok": True})
    path = next(p for p in store.docs if "/payload_chunks/" in p)
    store.docs[path]["data"] = "corrupt"
    assert cache.load_derived_payload("key") is None
    store.docs["dashboard_derived_payloads/key"]["schema_version"] = 1
    assert cache.load_derived_payload("key") is None


def test_callers_cannot_override_publication_fields(store):
    cache.save_derived_payload("key", {"ok": True}, metadata={"generation": "invalid", "chunk_count": 999})
    assert cache.load_derived_payload("key") == {"ok": True}
