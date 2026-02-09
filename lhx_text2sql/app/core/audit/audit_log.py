import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4


class AuditLogger:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def new_id() -> str:
        return str(uuid4())

    def write(
        self,
        audit_id: str,
        question: str,
        user_context: Dict[str, Any],
        evidence_summary: str,
        plan_initial: Optional[Dict[str, Any]],
        plan_final: Optional[Dict[str, Any]],
        validation_errors: list,
        sql: Optional[str],
        elapsed_ms: int,
        error: Optional[str],
    ) -> None:
        record = {
            "audit_log_id": audit_id,
            "timestamp": datetime.utcnow().isoformat(),
            "question": question,
            "user_context": user_context,
            "evidence_summary": evidence_summary,
            "plan_initial": plan_initial,
            "plan_final": plan_final,
            "validation_errors": validation_errors,
            "sql": sql,
            "elapsed_ms": elapsed_ms,
            "error": error,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
