from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from portfolio_backend.pipeline_snapshot_store import (
    FirestorePipelineSnapshotStore,
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
