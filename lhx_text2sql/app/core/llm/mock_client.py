import json
import re
from typing import Dict, Optional

from app.core.llm.client import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self, force_invalid: bool = False, force_sql: bool = False) -> None:
        self.force_invalid = force_invalid
        self.force_sql = force_sql

    def generate_json(self, prompt: str, schema: dict) -> Dict:
        if self.force_invalid:
            return "SELECT * FROM t" if self.force_sql else "not json"

        inputs = self._extract_inputs(prompt)
        question = inputs.get("question", "")
        evidence = inputs.get("evidence", {})
        time_range = inputs.get("time_range") or {"start": "2024-01-01", "end": "2024-01-31"}

        metric_candidates = evidence.get("metric_candidates", [])
        schema_candidates = evidence.get("schema_candidates", [])
        join_paths = evidence.get("join_paths", [])

        metric_id = self._pick_metric(metric_candidates, question)
        intent = self._pick_intent(question)
        join_path_id = join_paths[0]["join_path_id"] if join_paths else "NONE"
        dimension = self._pick_dimension(schema_candidates)

        time_grain = "day"
        for metric in metric_candidates:
            if metric.get("metric_id") == metric_id:
                time_grain = metric.get("default_time_grain", "day")

        plan = {
            "version": "1.0",
            "intent": intent,
            "metric_id": metric_id,
            "metric_params": {},
            "dimensions": [dimension] if dimension and intent in {"trend", "rank"} else [],
            "time_range": time_range,
            "time_grain": time_grain,
            "filters": [],
            "join_path_id": join_path_id,
            "sort": self._pick_sort(intent),
            "limit": 10 if intent == "rank" else 200,
            "output": self._pick_output(intent),
            "confidence": 0.6,
            "clarifications": [],
        }
        return plan

    def generate_text(self, prompt: str) -> str:
        return "这是示例回答，请接入真实 LLM 以获得更完整的自然语言答案。"

    @staticmethod
    def _extract_inputs(prompt: str) -> Dict:
        marker = "<INPUTS>"
        if marker in prompt:
            payload = prompt.split(marker, 1)[1].strip()
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return {}
        match = re.search(r"(\{.*\})", prompt, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _pick_metric(metric_candidates, question: str) -> str:
        if not metric_candidates:
            return "UNKNOWN"
        if "\u7ebf\u635f" in question:
            for m in metric_candidates:
                if "线损" in m.get("name", "") or "loss" in m.get("metric_id", ""):
                    return m.get("metric_id")
        if "\u8d1f\u8377" in question:
            for m in metric_candidates:
                if "负荷" in m.get("name", "") or "load" in m.get("metric_id", ""):
                    return m.get("metric_id")
        if "\u505c\u7535" in question:
            for m in metric_candidates:
                if "outage" in m.get("metric_id", ""):
                    return m.get("metric_id")
        if "\u8df3\u95f8" in question:
            for m in metric_candidates:
                if "trip" in m.get("metric_id", ""):
                    return m.get("metric_id")
        return metric_candidates[0].get("metric_id")

    @staticmethod
    def _pick_intent(question: str) -> str:
        if "排名" in question or "top" in question.lower():
            return "rank"
        if "对比" in question or "\u540c\u6bd4" in question or "\u73af\u6bd4" in question:
            return "compare"
        if "明细" in question:
            return "detail"
        if "趋势" in question:
            return "trend"
        return "aggregate"

    @staticmethod
    def _pick_dimension(schema_candidates) -> Optional[Dict[str, str]]:
        for item in schema_candidates:
            if item.get("field", "").endswith("_name"):
                return {"table": item.get("table"), "field": item.get("field")}
        if schema_candidates:
            item = schema_candidates[0]
            return {"table": item.get("table"), "field": item.get("field")}
        return None

    @staticmethod
    def _pick_sort(intent: str) -> Dict:
        if intent == "rank":
            return {"by": "metric", "order": "desc"}
        if intent == "trend":
            return {"by": "time_bucket", "order": "asc"}
        return {"by": "metric", "order": "desc"}

    @staticmethod
    def _pick_output(intent: str) -> Dict:
        if intent == "trend":
            return {"format": "table", "chart_suggest": "line"}
        if intent == "rank":
            return {"format": "table", "chart_suggest": "bar"}
        return {"format": "table", "chart_suggest": "none"}
