import json
from pathlib import Path
from typing import List

from app.core.models import MetricDef
from app.core.rag.vector_store import VectorStore


class MetricKB:
    def __init__(self, path: str, store: VectorStore) -> None:
        self.path = Path(path)
        self.store = store
        self.data: List[MetricDef] = []
        self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.data = [MetricDef(**item) for item in raw]
        for item in self.data:
            text = " ".join(
                [
                    item.metric_id,
                    item.name,
                    item.definition,
                    item.formula,
                    " ".join(item.required_fields),
                    item.default_time_grain,
                    item.unit,
                ]
            )
            doc_id = f"metric::{item.metric_id}"
            self.store.upsert(doc_id, text, item.dict())

    def query(self, text: str, top_k: int = 5) -> List[MetricDef]:
        docs = self.store.query(text, top_k=top_k)
        return [MetricDef(**doc.metadata) for doc in docs]
