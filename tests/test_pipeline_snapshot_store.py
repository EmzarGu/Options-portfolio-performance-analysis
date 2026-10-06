from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from portfolio_backend.pipeline_snapshot_store import (
    FirestorePipelineSnapshotStore,
    MemoryPipelineSnapshotStore,
    pipeline_snapshot_id,
)


class _FakeDocumentSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _FakeDocumentRef:
    def __init__(self, root, path):
        self._root = root
        self._path = tuple(path)

    def get(self):
        return _FakeDocumentSnapshot(self._root.get(self._path))

    def set(self, data, merge=False):
        if merge and self._path in self._root:
            self._root[self._path].update(data)
        else:
            self._root[self._path] = dict(data)

    def collection(self, name):
        return _FakeCollection(self._root, (*self._path, name))


class _FakeCollection:
    def __init__(self, root, path):
        self._root = root
        self._path = tuple(path)

    def document(self, doc_id):
        return _FakeDocumentRef(self._root, (*self._path, str(doc_id)))


class _FakeFirestoreClient:
    def __init__(self):
        self.docs = {}

    def collection(self, name):
        return _FakeCollection(self.docs, (name,))


def test_firestore_pipeline_snapshot_store_round_trips_chunked_state():
    client = _FakeFirestoreClient()
    store = FirestorePipelineSnapshotStore(client=client)
    state = SimpleNamespace(
        frame=pd.DataFrame({"ticker": ["FTNT", "FUTU"], "value": [1.25, -3.5]}),
        text="x" * 1_000_000,
    )
    snapshot_id = pipeline_snapshot_id(
        source_snapshot_id="ibkr-flex:1504277:run-1",
        as_of=pd.Timestamp("2026-05-13"),
        selected_sheets=["IBKR Flex"],
    )

    store.save(snapshot_id, state, {"source_snapshot_id": "ibkr-flex:1504277:run-1"})
    loaded = store.load(snapshot_id)

    assert loaded is not None
    assert loaded.snapshot_id == snapshot_id
    assert loaded.metadata["source_snapshot_id"] == "ibkr-flex:1504277:run-1"
    pd.testing.assert_frame_equal(loaded.state.frame, state.frame)
    assert loaded.state.text == state.text


def test_firestore_pipeline_snapshot_store_tracks_latest_pointer():
    client = _FakeFirestoreClient()
    store = FirestorePipelineSnapshotStore(client=client)
    state = SimpleNamespace(value=42)

    store.save_latest(
        "latest:demo",
        "snapshot:demo:1",
        state,
        {"kind": "refreshed", "source_snapshot_id": "source-1", "cache_bust": 123},
    )
    loaded = store.load_latest("latest:demo")

    assert loaded is not None
    assert loaded.snapshot_id == "snapshot:demo:1"
    assert loaded.state.value == 42
    assert loaded.metadata["kind"] == "refreshed"
    pointer_doc = client.docs[("app_metadata", "latest:demo")]
    assert pointer_doc["snapshot_id"] == "snapshot:demo:1"
    assert pointer_doc["source_snapshot_id"] == "source-1"
    assert pointer_doc["cache_bust"] == 123


def test_memory_pipeline_snapshot_store_tracks_latest_pointer():
    store = MemoryPipelineSnapshotStore()
    state = SimpleNamespace(value=42)

    store.save_latest("latest:demo", "snapshot:demo:1", state, {"kind": "refreshed"})
    loaded = store.load_latest("latest:demo")

    assert loaded is not None
    assert loaded.snapshot_id == "snapshot:demo:1"
    assert loaded.state is state
    assert loaded.metadata == {"kind": "refreshed"}


def test_memory_pipeline_snapshot_store_build_lease_blocks_other_owner_until_release():
    store = MemoryPipelineSnapshotStore()

    assert store.try_acquire_build_lease("lease:demo", "owner-a", ttl_seconds=30)
    assert not store.try_acquire_build_lease("lease:demo", "owner-b", ttl_seconds=30)
    assert store.try_acquire_build_lease("lease:demo", "owner-a", ttl_seconds=30)

    store.release_build_lease("lease:demo", "owner-b")
    assert not store.try_acquire_build_lease("lease:demo", "owner-b", ttl_seconds=30)

    store.release_build_lease("lease:demo", "owner-a")
    assert store.try_acquire_build_lease("lease:demo", "owner-b", ttl_seconds=30)


def test_accounting_revision_rejects_old_snapshot_and_changes_cache_identity(monkeypatch):
    import portfolio_backend.pipeline_snapshot_store as module

    client = _FakeFirestoreClient()
    store = FirestorePipelineSnapshotStore(client=client)
    args = dict(source_snapshot_id="unchanged-ibkr-import", as_of="2026-10-01", selected_sheets=["IBKR Flex"])
    legacy_version = 7
    with monkeypatch.context() as patch:
        patch.setattr(module, "SNAPSHOT_SCHEMA_VERSION", legacy_version)
        old_id = pipeline_snapshot_id(**args)
        store.save(old_id, SimpleNamespace(realized_options_pnl=6510.11), {})
    assert module.SNAPSHOT_SCHEMA_VERSION > legacy_version
    assert pipeline_snapshot_id(**args) != old_id
    assert store.load(old_id) is None
