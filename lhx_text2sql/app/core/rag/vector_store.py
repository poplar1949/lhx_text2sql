from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Document:
    doc_id: str
    text: str
    metadata: Dict[str, Any]
    score: float


class VectorStore(ABC):
    @abstractmethod
    def upsert(self, doc_id: str, text: str, metadata: Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def query(
        self, text: str, top_k: int = 5, filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        raise NotImplementedError
