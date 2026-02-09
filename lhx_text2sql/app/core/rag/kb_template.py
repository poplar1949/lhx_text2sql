import json
from pathlib import Path
from typing import List

from app.core.models import TemplateRule
from app.core.rag.vector_store import VectorStore


class TemplateKB:
    def __init__(self, path: str, store: VectorStore) -> None:
        self.path = Path(path)
        self.store = store
        self.data: List[TemplateRule] = []
        self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self.data = [TemplateRule(**item) for item in raw]
        for item in self.data:
            text = " ".join(
                [
                    item.template_id,
                    item.intent,
                    " ".join(item.allowed_aggs),
                    " ".join(item.allowed_funcs),
                    " ".join(item.required_clauses),
                ]
            )
            doc_id = f"template::{item.template_id}"
            self.store.upsert(doc_id, text, item.dict())

    def query(self, text: str, top_k: int = 5) -> List[TemplateRule]:
        docs = self.store.query(text, top_k=top_k)
        return [TemplateRule(**doc.metadata) for doc in docs]
