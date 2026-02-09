from datetime import date
from typing import Dict, List

from app.core.models import EvidenceBundle, ValidationError
from app.core.schema import validate_plan


TIME_FIELD_NAMES = {"ts", "timestamp", "event_time", "date", "dt"}
TIME_DATA_TYPES = {"datetime", "timestamp", "date"}


class PlanValidator:
    def __init__(self, schema: dict) -> None:
        self.schema = schema

    def validate(self, plan: Dict, evidence: EvidenceBundle) -> List[ValidationError]:
        if not isinstance(plan, dict):
            return [
                ValidationError(
                    code="not_json",
                    message="Plan is not a JSON object",
                    field_path="$",
                    suggestions=[],
                )
            ]

        errors = validate_plan(plan, self.schema)
        if errors:
            return errors

        metric_ids = {m.metric_id for m in evidence.metric_candidates}
        if plan.get("metric_id") not in metric_ids:
            errors.append(
                ValidationError(
                    code="metric_not_found",
                    message="metric_id not in candidates",
                    field_path="metric_id",
                    suggestions=sorted(metric_ids),
                )
            )

        schema_fields = {f"{s.table}.{s.field}" for s in evidence.schema_candidates}
        for idx, dim in enumerate(plan.get("dimensions", [])):
            key = f"{dim.get('table')}.{dim.get('field')}"
            if key not in schema_fields:
                errors.append(
                    ValidationError(
                        code="dimension_field_invalid",
                        message=f"Dimension field {key} not in schema candidates",
                        field_path=f"dimensions[{idx}]",
                        suggestions=sorted(schema_fields)[:5],
                    )
                )

        for idx, fil in enumerate(plan.get("filters", [])):
            key = f"{fil.get('table')}.{fil.get('field')}"
            if key not in schema_fields:
                errors.append(
                    ValidationError(
                        code="filter_field_invalid",
                        message=f"Filter field {key} not in schema candidates",
                        field_path=f"filters[{idx}]",
                        suggestions=sorted(schema_fields)[:5],
                    )
                )

        join_ids = {j.join_path_id for j in evidence.join_paths}
        join_path_id = plan.get("join_path_id")
        if join_path_id != "NONE" and join_path_id not in join_ids:
            referenced_tables = self._collect_tables(plan, evidence)
            suggestions = self._suggest_join_paths(referenced_tables, evidence) or sorted(join_ids)
            errors.append(
                ValidationError(
                    code="join_path_not_found",
                    message="join_path_id not in candidates",
                    field_path="join_path_id",
                    suggestions=suggestions,
                )
            )
        else:
            join_errors = self._check_join_reachability(plan, evidence)
            errors.extend(join_errors)

        time_range = plan.get("time_range") or {}
        start = time_range.get("start")
        end = time_range.get("end")
        if start and end:
            try:
                start_date = date.fromisoformat(start)
                end_date = date.fromisoformat(end)
                if start_date > end_date:
                    errors.append(
                        ValidationError(
                            code="time_range_invalid",
                            message="time_range.start is after end",
                            field_path="time_range",
                            suggestions=[],
                        )
                    )
            except ValueError:
                errors.append(
                    ValidationError(
                        code="time_range_invalid",
                        message="time_range must be YYYY-MM-DD",
                        field_path="time_range",
                        suggestions=["YYYY-MM-DD"],
                    )
                )
        else:
            errors.append(
                ValidationError(
                    code="time_range_missing",
                    message="time_range is required",
                    field_path="time_range",
                    suggestions=[],
                )
            )

        if plan.get("intent") == "trend" and not plan.get("time_grain"):
            errors.append(
                ValidationError(
                    code="time_grain_required",
                    message="time_grain required for trend intent",
                    field_path="time_grain",
                    suggestions=[
                        "15m",
                        "hour",
                        "day",
                        "week",
                        "month",
                    ],
                )
            )

        if not self._has_time_field(evidence):
            errors.append(
                ValidationError(
                    code="time_field_missing",
                    message="No time field found in schema candidates",
                    field_path="time_range",
                    suggestions=[],
                )
            )

        template_errors = self._check_template_rules(plan, evidence)
        errors.extend(template_errors)

        return errors

    @staticmethod
    def _has_time_field(evidence: EvidenceBundle) -> bool:
        for item in evidence.schema_candidates:
            if item.field.lower() in TIME_FIELD_NAMES:
                return True
            if item.data_type.lower() in TIME_DATA_TYPES:
                return True
        for metric in evidence.metric_candidates:
            for field in metric.required_fields:
                if "." in field:
                    _, fld = field.split(".", 1)
                else:
                    fld = field
                if fld.lower() in TIME_FIELD_NAMES:
                    return True
        return False

    @staticmethod
    def _check_template_rules(plan: Dict, evidence: EvidenceBundle) -> List[ValidationError]:
        errors: List[ValidationError] = []
        intent = plan.get("intent")
        for rule in evidence.template_rules:
            if rule.intent != intent:
                continue
            required_funcs = PlanValidator._required_funcs(plan)
            if required_funcs and not set(required_funcs).issubset(set(rule.allowed_funcs)):
                errors.append(
                    ValidationError(
                        code="function_not_allowed",
                        message="Required functions not in allowlist",
                        field_path="time_grain",
                        suggestions=rule.allowed_funcs,
                    )
                )
            required_aggs = PlanValidator._required_aggs(plan, evidence)
            if required_aggs and not set(required_aggs).issubset(set(rule.allowed_aggs)):
                errors.append(
                    ValidationError(
                        code="agg_not_allowed",
                        message="Required aggregations not in allowlist",
                        field_path="metric_id",
                        suggestions=rule.allowed_aggs,
                    )
                )
            for clause in rule.required_clauses:
                if clause == "time_range" and not plan.get("time_range"):
                    errors.append(
                        ValidationError(
                            code="required_clause_missing",
                            message="time_range required by template",
                            field_path="time_range",
                            suggestions=[],
                        )
                    )
                if clause == "time_grain" and not plan.get("time_grain"):
                    errors.append(
                        ValidationError(
                            code="required_clause_missing",
                            message="time_grain required by template",
                            field_path="time_grain",
                            suggestions=[rule.intent],
                        )
                    )
                if clause == "group_by_time" and not plan.get("time_grain"):
                    errors.append(
                        ValidationError(
                            code="required_clause_missing",
                            message="group_by_time required by template",
                            field_path="time_grain",
                            suggestions=["day"],
                        )
                    )
                if clause == "order_by" and not plan.get("sort"):
                    errors.append(
                        ValidationError(
                            code="required_clause_missing",
                            message="sort required by template",
                            field_path="sort",
                            suggestions=["metric desc"],
                        )
                    )
                if clause == "limit" and not plan.get("limit"):
                    errors.append(
                        ValidationError(
                            code="required_clause_missing",
                            message="limit required by template",
                            field_path="limit",
                            suggestions=["10"],
                        )
                    )
        return errors

    @staticmethod
    def _required_funcs(plan: Dict) -> List[str]:
        if plan.get("intent") != "trend":
            return []
        grain = plan.get("time_grain")
        if grain == "15m":
            return ["from_unixtime", "unix_timestamp"]
        if grain in {"hour", "day", "month"}:
            return ["date_format"]
        if grain == "week":
            return ["yearweek"]
        return []

    @staticmethod
    def _required_aggs(plan: Dict, evidence: EvidenceBundle) -> List[str]:
        metric_id = plan.get("metric_id")
        if not metric_id:
            return []
        for metric in evidence.metric_candidates:
            if metric.metric_id == metric_id:
                return ["sum"]
        return []

    def _check_join_reachability(
        self, plan: Dict, evidence: EvidenceBundle
    ) -> List[ValidationError]:
        errors: List[ValidationError] = []
        referenced_tables = self._collect_tables(plan, evidence)
        if not referenced_tables:
            return errors

        join_path_id = plan.get("join_path_id")
        if join_path_id == "NONE":
            if len(referenced_tables) > 1:
                suggestions = self._suggest_join_paths(referenced_tables, evidence)
                errors.append(
                    ValidationError(
                        code="join_required",
                        message="Multiple tables referenced but join_path_id is NONE",
                        field_path="join_path_id",
                        suggestions=suggestions,
                    )
                )
            return errors

        join_path = next(
            (jp for jp in evidence.join_paths if jp.join_path_id == join_path_id), None
        )
        if join_path and not referenced_tables.issubset(set(join_path.tables)):
            suggestions = self._suggest_join_paths(referenced_tables, evidence)
            errors.append(
                ValidationError(
                    code="join_path_unreachable",
                    message="join_path_id does not cover all referenced tables",
                    field_path="join_path_id",
                    suggestions=suggestions,
                )
            )
        return errors

    def _collect_tables(self, plan: Dict, evidence: EvidenceBundle) -> set:
        tables = set()
        for dim in plan.get("dimensions", []):
            table = dim.get("table")
            if table:
                tables.add(table)
        for fil in plan.get("filters", []):
            table = fil.get("table")
            if table:
                tables.add(table)

        metric_id = plan.get("metric_id")
        metric_tables = set()
        for metric in evidence.metric_candidates:
            if metric.metric_id == metric_id:
                for field in metric.required_fields:
                    if "." in field:
                        table, _ = field.split(".", 1)
                        tables.add(table)
                        metric_tables.add(table)
                break

        time_table = self._pick_time_table(evidence, metric_tables)
        if time_table:
            tables.add(time_table)
        return tables

    @staticmethod
    def _pick_time_table(evidence: EvidenceBundle, preferred_tables: set) -> str:
        if preferred_tables:
            for item in evidence.schema_candidates:
                if item.table in preferred_tables:
                    if item.field.lower() in TIME_FIELD_NAMES:
                        return item.table
                    if item.data_type.lower() in TIME_DATA_TYPES:
                        return item.table
        for item in evidence.schema_candidates:
            if item.field.lower() in TIME_FIELD_NAMES:
                return item.table
            if item.data_type.lower() in TIME_DATA_TYPES:
                return item.table
        return ""

    @staticmethod
    def _suggest_join_paths(referenced_tables: set, evidence: EvidenceBundle) -> List[str]:
        suggestions: List[str] = []
        for jp in evidence.join_paths:
            if referenced_tables.issubset(set(jp.tables)):
                suggestions.append(jp.join_path_id)
        return suggestions
