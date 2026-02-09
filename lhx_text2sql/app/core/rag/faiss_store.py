import re
from typing import Any, Dict, List, Optional

from app.core.rag.vector_store import Document, VectorStore


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-zA-Z0-9_]+|[\u4e00-\u9fff]", text.lower())


class SimpleInMemoryVectorStore(VectorStore):
    def __init__(self) -> None:
        self._docs: Dict[str, Dict[str, Any]] = {}
        self._tokens: Dict[str, set] = {}

    def upsert(self, doc_id: str, text: str, metadata: Dict[str, Any]) -> None:
        tokens = set(_tokenize(text))
        self._docs[doc_id] = {"text": text, "metadata": metadata}
        self._tokens[doc_id] = tokens

    def query(
        self, text: str, top_k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        q_tokens = set(_tokenize(text))
        if not q_tokens:
            return []
        scored: List[Document] = []
        for doc_id, tokens in self._tokens.items():
            metadata = self._docs[doc_id]["metadata"]
            if filter:
                if any(metadata.get(k) != v for k, v in filter.items()):
                    continue
            score = self._cosine_sim(q_tokens, tokens)
            if score > 0:
                scored.append(
                    Document(
                        doc_id=doc_id,
                        text=self._docs[doc_id]["text"],
                        metadata=metadata,
                        score=score,
                    )
                )
        scored.sort(key=lambda d: d.score, reverse=True)
        return scored[:top_k]

    @staticmethod
    def _cosine_sim(a: set, b: set) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        return inter / ((len(a) ** 0.5) * (len(b) ** 0.5))
