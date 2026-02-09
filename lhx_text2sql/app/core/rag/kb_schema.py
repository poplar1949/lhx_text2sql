import json
from pathlib import Path
from typing import List

from app.core.models import SchemaEntity
from app.core.rag.vector_store import VectorStore


class SchemaKB:
    def __init__(self, path: str, store: VectorStore) -> None:
        self.path = Path(path)
        self.store = store
        self.data: List[SchemaEntity] = []
        self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.data = [SchemaEntity(**item) for item in raw]
        for item in self.data:
            text = " ".join(
                [
                    item.table,
                    item.field,
                    item.field_desc,
                    " ".join(item.aliases),
                    item.unit,
                    item.data_type,
                    " ".join(item.quality_tags),
                ]
            )
            doc_id = f"schema::{item.table}.{item.field}"
            self.store.upsert(doc_id, text, item.dict())

    def query(self, text: str, top_k: int = 5) -> List[SchemaEntity]:
        docs = self.store.query(text, top_k=top_k)
        return [SchemaEntity(**doc.metadata) for doc in docs]
