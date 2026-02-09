import json
from pathlib import Path
from typing import List

from app.core.models import JoinPath
from app.core.rag.vector_store import VectorStore


class JoinGraphKB:
    def __init__(self, path: str, store: VectorStore) -> None:
        self.path = Path(path)
        self.store = store
        self.data: List[JoinPath] = []
        self.graph: dict = {}
        self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.data = [JoinPath(**item) for item in raw]
        self.graph = {}
        for item in self.data:
            text = " ".join(
                [
                    item.join_path_id,
                    item.description,
                    " ".join(item.tables),
                ]
            )
            doc_id = f"join::{item.join_path_id}"
            self.store.upsert(doc_id, text, item.dict())
            for edge in item.edges:
                self._add_edge(edge.left_table, edge.right_table)
                self._add_edge(edge.right_table, edge.left_table)

    def _add_edge(self, left: str, right: str) -> None:
        if left not in self.graph:
            self.graph[left] = set()
        self.graph[left].add(right)

    def query(self, text: str, top_k: int = 5) -> List[JoinPath]:
        docs = self.store.query(text, top_k=top_k)
        return [JoinPath(**doc.metadata) for doc in docs]
