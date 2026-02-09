import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from app.core.config import Settings
from app.core.llm.client import LLMClient
from app.core.models import EvidenceBundle, MetricDef, PlanDSL, ValidationError
from app.core.planning.repair import PlanRepair
from app.core.planning.validator import PlanValidator
from app.core.rag.kb_join import JoinGraphKB
from app.core.rag.kb_metric import MetricKB
from app.core.rag.kb_schema import SchemaKB
from app.core.rag.kb_template import TemplateKB


SQL_KEYWORDS = re.compile(
    r"\b(select|from|where|join|group\s+by|order\s+by|insert|update|delete)\b",
    re.IGNORECASE,
)


@dataclass
class PlanResult:
    plan: PlanDSL
    plan_initial: Dict[str, Any]
    evidence: EvidenceBundle
    evidence_summary: str
    validation_errors: List[ValidationError]
    metric_def: MetricDef


class Planner:
    def __init__(
        self,
        settings: Settings,
        llm_client: LLMClient,
        schema_kb: SchemaKB,
        join_kb: JoinGraphKB,
        metric_kb: MetricKB,
        template_kb: TemplateKB,
        validator: PlanValidator,
        repairer: PlanRepair,
        prompt_path: str,
    ) -> None:
        self.settings = settings
        self.llm_client = llm_client
        self.schema_kb = schema_kb
        self.join_kb = join_kb
        self.metric_kb = metric_kb
        self.template_kb = template_kb
        self.validator = validator
        self.repairer = repairer
        self.prompt_template = self._load_prompt(prompt_path)

    @staticmethod
    def _load_prompt(path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def generate_plan(
        self,
        question: str,
        user_context: Dict[str, Any],
        time_range: Optional[Dict[str, str]] = None,
    ) -> PlanResult:
        slots = self._parse_slots(question)
        evidence = self._retrieve(question, slots, self.settings.rag_top_k)
        if self.settings.llm_mode == "no_llm":
            evidence = self._ensure_no_llm_evidence(evidence)
            plan = self._build_fixed_plan(evidence, time_range)
            plan_initial = plan
            validation_errors: List[ValidationError] = []
        else:
            plan = self._call_llm(question, user_context, time_range, evidence)
            plan_initial = plan
            validation_errors = self.validator.validate(plan, evidence)
            if self._has_error(validation_errors, "metric_not_found"):
                evidence = EvidenceBundle(
                    metric_candidates=self.metric_kb.data,
                    schema_candidates=evidence.schema_candidates,
                    join_paths=evidence.join_paths,
                    template_rules=evidence.template_rules,
                )
                fixed_metric_id = self._auto_fix_metric_id(question, self.metric_kb.data)
                if fixed_metric_id:
                    plan["metric_id"] = fixed_metric_id
                validation_errors = self.validator.validate(plan, evidence)

        if validation_errors:
            evidence = self._augment_evidence_for_errors(evidence, validation_errors)
            suggestions = self._collect_suggestions(validation_errors)
            refine_query = " ".join([question] + suggestions)
            refine_slots = self._parse_slots(refine_query)
            evidence = self._retrieve(
                refine_query, refine_slots, self.settings.rag_top_k_second
            )
            plan = self.repairer.repair(plan, [e.dict() for e in validation_errors], evidence)
            validation_errors = self.validator.validate(plan, evidence)
            if self._has_error(validation_errors, "metric_not_found"):
                evidence = EvidenceBundle(
                    metric_candidates=self.metric_kb.data,
                    schema_candidates=evidence.schema_candidates,
                    join_paths=evidence.join_paths,
                    template_rules=evidence.template_rules,
                )
                fixed_metric_id = self._auto_fix_metric_id(question, self.metric_kb.data)
                if fixed_metric_id:
                    plan["metric_id"] = fixed_metric_id
                validation_errors = self.validator.validate(plan, evidence)

        if validation_errors:
            raise ValueError(
                "Plan validation failed: "
                + "; ".join([e.message for e in validation_errors])
            )

        if self._contains_sql_keywords(json.dumps(plan, ensure_ascii=False)):
            raise ValueError("LLM output contains SQL keywords")

        plan_obj = PlanDSL.parse_obj(plan)
        metric_def = self._get_metric_def(plan_obj.metric_id, evidence)
        evidence_summary = self._summarize_evidence(evidence)
        return PlanResult(
            plan=plan_obj,
            plan_initial=plan_initial,
            evidence=evidence,
            evidence_summary=evidence_summary,
            validation_errors=validation_errors,
            metric_def=metric_def,
        )

    def _call_llm(
        self,
        question: str,
        user_context: Dict[str, Any],
        time_range: Optional[Dict[str, str]],
        evidence: EvidenceBundle,
    ) -> Dict[str, Any]:
        payload = {
            "question": question,
            "user_context": user_context,
            "time_range": time_range,
            "evidence": evidence.dict(),
        }
        prompt = f"{self.prompt_template}\n\n<INPUTS>\n{json.dumps(payload, ensure_ascii=False)}"
        try:
            plan = self.llm_client.generate_json(prompt=prompt, schema=self.validator.schema)
        except Exception as exc:
            if self.settings.llm_plan_retry_on_timeout and self._is_timeout_error(exc):
                trimmed = self._trim_evidence(evidence, self.settings.llm_plan_trim_top_k)
                small_payload = {
                    "question": question,
                    "user_context": user_context,
                    "time_range": time_range,
                    "evidence": trimmed,
                }
                small_prompt = (
                    f"{self.prompt_template}\n\n<INPUTS_TRIMMED>\n"
                    f"{json.dumps(small_payload, ensure_ascii=False)}"
                )
                try:
                    plan = self.llm_client.generate_json(
                        prompt=small_prompt, schema=self.validator.schema
                    )
                except Exception as exc2:
                    raise ValueError(
                        f"LLM plan generation failed (timeout, trimmed retry failed): {exc2}"
                    ) from exc2
            else:
                raise ValueError(f"LLM plan generation failed: {exc}") from exc
        if not isinstance(plan, dict):
            if isinstance(plan, str) and self._contains_sql_keywords(plan):
                raise ValueError("LLM output contains SQL keywords")
            raise ValueError("LLM output is not JSON")
        return plan

    def _retrieve(self, text: str, slots: Dict[str, List[str]], top_k: int) -> EvidenceBundle:
        metric_query = self._build_query(text, slots.get("metric_terms", []))
        schema_query = self._build_query(text, slots.get("schema_terms", []))
        join_query = self._build_query(
            text, slots.get("object_terms", []) + slots.get("schema_terms", [])
        )
        template_query = self._build_query(text, slots.get("intent_terms", []))

        metric_candidates = self.metric_kb.query(metric_query, top_k=top_k)
        schema_candidates = self.schema_kb.query(schema_query, top_k=top_k)
        schema_candidates = self._ensure_time_fields(schema_candidates)
        join_paths = self.join_kb.query(join_query, top_k=top_k)
        template_rules = self.template_kb.query(template_query, top_k=top_k)
        return EvidenceBundle(
            metric_candidates=metric_candidates,
            schema_candidates=schema_candidates,
            join_paths=join_paths,
            template_rules=template_rules,
        )

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "timeout" in msg or "timed out" in msg

    @staticmethod
    def _trim_evidence(evidence: EvidenceBundle, top_k: int) -> Dict[str, Any]:
        top_k = max(1, int(top_k))
        metrics = []
        for item in evidence.metric_candidates[:top_k]:
            metrics.append(
                {
                    "metric_id": item.metric_id,
                    "name": item.name,
                    "definition": item.definition,
                    "default_time_grain": item.default_time_grain,
                    "unit": item.unit,
                    "required_fields": item.required_fields,
                }
            )
        schemas = []
        for item in evidence.schema_candidates[:top_k]:
            schemas.append(
                {
                    "table": item.table,
                    "field": item.field,
                    "field_desc": item.field_desc,
                    "aliases": item.aliases,
                    "unit": item.unit,
                    "data_type": item.data_type,
                    "quality_tags": item.quality_tags,
                }
            )
        joins = []
        for item in evidence.join_paths[:top_k]:
            joins.append(
                {
                    "join_path_id": item.join_path_id,
                    "description": item.description,
                    "tables": item.tables,
                    "edges": [edge.dict() for edge in item.edges],
                }
            )
        templates = []
        for item in evidence.template_rules[:top_k]:
            templates.append(
                {
                    "template_id": item.template_id,
                    "intent": item.intent,
                    "allowed_aggs": item.allowed_aggs,
                    "allowed_funcs": item.allowed_funcs,
                    "required_clauses": item.required_clauses,
                }
            )
        return {
            "metric_candidates": metrics,
            "schema_candidates": schemas,
            "join_paths": joins,
            "template_rules": templates,
        }

    @staticmethod
    def _collect_suggestions(errors: List[ValidationError]) -> List[str]:
        suggestions: List[str] = []
        for err in errors:
            suggestions.extend(err.suggestions)
        return suggestions[:8]

    @staticmethod
    def _summarize_evidence(evidence: EvidenceBundle) -> str:
        metrics = ",".join([m.metric_id for m in evidence.metric_candidates])
        fields = ",".join([f"{s.table}.{s.field}" for s in evidence.schema_candidates])
        joins = ",".join([j.join_path_id for j in evidence.join_paths])
        templates = ",".join([t.template_id for t in evidence.template_rules])
        return f"metrics=[{metrics}] schema=[{fields}] joins=[{joins}] templates=[{templates}]"

    @staticmethod
    def _contains_sql_keywords(text: str) -> bool:
        return bool(SQL_KEYWORDS.search(text))

    @staticmethod
    def _get_metric_def(metric_id: str, evidence: EvidenceBundle) -> MetricDef:
        for metric in evidence.metric_candidates:
            if metric.metric_id == metric_id:
                return metric
        raise ValueError("metric_id not found in evidence")

    def _build_fixed_plan(
        self, evidence: EvidenceBundle, time_range: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        if not time_range or not time_range.get("start") or not time_range.get("end"):
            raise ValueError("no_llm mode requires time_range")
        if not evidence.metric_candidates:
            raise ValueError("no_llm mode requires metric candidates")
        metric = self._pick_fixed_metric(evidence)
        tables = set()
        for field in metric.required_fields:
            if "." in field:
                table, _ = field.split(".", 1)
                tables.add(table)
        time_table = self._pick_time_table(evidence)
        if time_table:
            tables.add(time_table)

        join_path_id = "NONE"
        if len(tables) > 1:
            for jp in evidence.join_paths:
                if tables.issubset(set(jp.tables)):
                    join_path_id = jp.join_path_id
                    break
            if join_path_id == "NONE":
                raise ValueError("no_llm mode cannot find join_path for required tables")

        return {
            "version": "1.0",
            "intent": "aggregate",
            "metric_id": metric.metric_id,
            "metric_params": {},
            "dimensions": [],
            "time_range": {"start": time_range["start"], "end": time_range["end"]},
            "time_grain": metric.default_time_grain or "day",
            "filters": [],
            "join_path_id": join_path_id,
            "sort": None,
            "limit": 200,
            "output": {"format": "single_value", "chart_suggest": "none"},
            "confidence": 0.1,
            "clarifications": ["no_llm mode: fixed plan for SQL/DB test"],
            "errors_unresolved": [],
        }

    def _pick_fixed_metric(self, evidence: EvidenceBundle) -> MetricDef:
        fixed = (self.settings.fixed_metric_id or "").strip()
        if fixed:
            for item in evidence.metric_candidates:
                if item.metric_id == fixed:
                    return item
            for item in self.metric_kb.data:
                if item.metric_id == fixed:
                    return item
            raise ValueError(f"no_llm mode fixed_metric_id not found: {fixed}")
        return evidence.metric_candidates[0]

    def _ensure_no_llm_evidence(self, evidence: EvidenceBundle) -> EvidenceBundle:
        metric_candidates = evidence.metric_candidates or self.metric_kb.data
        schema_candidates = evidence.schema_candidates or self.schema_kb.data
        join_paths = evidence.join_paths or self.join_kb.data
        template_rules = evidence.template_rules or self.template_kb.data
        return EvidenceBundle(
            metric_candidates=metric_candidates,
            schema_candidates=schema_candidates,
            join_paths=join_paths,
            template_rules=template_rules,
        )

    @staticmethod
    def _pick_time_table(evidence: EvidenceBundle) -> str:
        for item in evidence.schema_candidates:
            if item.field.lower() in {"ts", "timestamp", "event_time", "date", "dt"}:
                return item.table
            if item.data_type.lower() in {"datetime", "timestamp", "date"}:
                return item.table
        return ""

    def _augment_evidence_for_errors(
        self, evidence: EvidenceBundle, errors: List[ValidationError]
    ) -> EvidenceBundle:
        error_codes = {e.code for e in errors}
        metric_candidates = evidence.metric_candidates
        schema_candidates = evidence.schema_candidates
        if "metric_not_found" in error_codes and not metric_candidates:
            metric_candidates = self.metric_kb.data
        if "time_field_missing" in error_codes:
            schema_candidates = self._ensure_time_fields(schema_candidates, force_all=True)
        return EvidenceBundle(
            metric_candidates=metric_candidates,
            schema_candidates=schema_candidates,
            join_paths=evidence.join_paths,
            template_rules=evidence.template_rules,
        )

    def _ensure_time_fields(
        self, schema_candidates: List, force_all: bool = False
    ) -> List:
        if not force_all and any(
            item.field.lower() in {"ts", "timestamp", "event_time", "date", "dt"}
            or item.data_type.lower() in {"datetime", "timestamp", "date"}
            for item in schema_candidates
        ):
            return schema_candidates
        time_fields = [
            item
            for item in self.schema_kb.data
            if item.field.lower() in {"ts", "timestamp", "event_time", "date", "dt"}
            or item.data_type.lower() in {"datetime", "timestamp", "date"}
        ]
        merged = {f"{i.table}.{i.field}": i for i in schema_candidates}
        for item in time_fields:
            merged.setdefault(f"{item.table}.{item.field}", item)
        return list(merged.values())

    def _parse_slots(self, question: str) -> Dict[str, List[str]]:
        norm = question.lower()
        metric_terms: List[str] = []
        schema_terms: List[str] = []
        object_terms: List[str] = []
        intent_terms: List[str] = []

        for metric in self.metric_kb.data:
            for term in [metric.metric_id, metric.name]:
                if term and term.lower() in norm:
                    metric_terms.append(term)

        for item in self.schema_kb.data:
            candidates: Sequence[str] = (
                [item.table, item.field, item.field_desc] + item.aliases
            )
            if any(term and term.lower() in norm for term in candidates):
                schema_terms.append(f"{item.table}.{item.field}")
                object_terms.append(item.table)

        intent = self._detect_intent(norm)
        if intent:
            intent_terms.append(intent)

        return {
            "metric_terms": self._dedupe(metric_terms),
            "schema_terms": self._dedupe(schema_terms),
            "object_terms": self._dedupe(object_terms),
            "intent_terms": intent_terms,
        }

    @staticmethod
    def _detect_intent(text: str) -> str:
        if any(k in text for k in ["\u6392\u540d", "top", "rank"]):
            return "rank"
        if any(k in text for k in ["\u8d8b\u52bf", "trend"]):
            return "trend"
        if any(k in text for k in ["\u5bf9\u6bd4", "\u540c\u6bd4", "\u73af\u6bd4", "compare"]):
            return "compare"
        if any(k in text for k in ["\u660e\u7ec6", "detail"]):
            return "detail"
        return ""

    @staticmethod
    def _build_query(text: str, terms: List[str]) -> str:
        if terms:
            return " ".join(terms + [text])
        return text

    @staticmethod
    def _auto_fix_metric_id(question: str, metrics: List[MetricDef]) -> str:
        q = question.lower()
        best_score = -1
        best_id = ""
        for metric in metrics:
            score = 0
            text = " ".join(
                [
                    metric.metric_id,
                    metric.name,
                    metric.definition,
                    metric.formula,
                    " ".join(metric.required_fields),
                ]
            ).lower()
            for token in Planner._simple_tokens(q):
                if token and token in text:
                    score += 2
            if any(k in q for k in ["金额", "费用", "cost", "amount"]) and (
                "amount" in text or "total_amount" in text
            ):
                score += 5
            if any(k in q for k in ["用电量", "用电", "电量", "consumption", "kwh"]) and (
                "consumption" in text
            ):
                score += 5
            if "账单" in q and "bills." in text:
                score += 3
            if score > best_score:
                best_score = score
                best_id = metric.metric_id
        return best_id

    @staticmethod
    def _simple_tokens(text: str) -> List[str]:
        return re.findall(r"[a-zA-Z0-9_]+|[\u4e00-\u9fff]", text)

    @staticmethod
    def _has_error(errors: List[ValidationError], code: str) -> bool:
        return any(err.code == code for err in errors)

    @staticmethod
    def _dedupe(items: List[str]) -> List[str]:
        seen = set()
        output: List[str] = []
        for item in items:
            if item not in seen:
                seen.add(item)
                output.append(item)
        return output
