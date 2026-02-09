from typing import List, Optional, Tuple

from sqlglot import exp, parse_one

from app.core.models import EvidenceBundle, MetricDef, PlanDSL


JOIN_TYPE_MAP = {
    "inner": "INNER",
    "left": "LEFT",
    "right": "RIGHT",
}


class SqlCompiler:
    def compile(self, plan: PlanDSL, evidence: EvidenceBundle) -> str:
        allowed_fields = self._build_allowed_fields(plan, evidence)
        metric_def = self._get_metric_def(plan.metric_id, evidence)
        time_table, time_field = self._pick_time_field(evidence, metric_def)
        base_table = self._pick_base_table(plan, evidence, metric_def, time_table)

        select_exprs: List[exp.Expression] = []
        group_exprs: List[exp.Expression] = []

        if plan.intent == "trend":
            time_expr = self._time_bucket_expr(time_table, time_field, plan.time_grain)
            time_alias = exp.alias_(time_expr, "time_bucket")
            select_exprs.append(time_alias)
            group_exprs.append(exp.column("time_bucket"))

        for dim in plan.dimensions:
            key = f"{dim.table}.{dim.field}"
            if key not in allowed_fields:
                raise ValueError(f"Dimension field not allowed: {key}")
            col_expr = exp.column(dim.field, table=dim.table)
            select_exprs.append(col_expr)
            group_exprs.append(col_expr)

        metric_expr = self._metric_expr(metric_def)
        select_exprs.append(exp.alias_(metric_expr, plan.metric_id))

        query = exp.select(*select_exprs).from_(base_table)

        join_path = self._get_join_path(plan.join_path_id, evidence)
        if join_path:
            for edge in join_path.edges:
                join_type = JOIN_TYPE_MAP.get(edge.join_type.lower(), "INNER")
                on_expr = exp.EQ(
                    this=exp.column(edge.left_field, table=edge.left_table),
                    expression=exp.column(edge.right_field, table=edge.right_table),
                )
                query = query.join(edge.right_table, on=on_expr, join_type=join_type)

        where_expr = self._time_range_filter(time_table, time_field, plan)
        for fil in plan.filters:
            key = f"{fil.table}.{fil.field}"
            if key not in allowed_fields:
                raise ValueError(f"Filter field not allowed: {key}")
            where_expr = self._and(where_expr, self._filter_expr(fil))
        if where_expr is not None:
            query = query.where(where_expr)

        if group_exprs:
            query = query.group_by(*group_exprs)

        if plan.sort:
            order_expr = self._order_expr(plan, allowed_fields)
            if order_expr is not None:
                query = query.order_by(order_expr)
        elif plan.intent == "trend":
            # Ensure stable time ordering for trend queries.
            query = query.order_by(exp.Ordered(this=exp.column("time_bucket"), desc=False))

        limit = plan.limit or 200
        query = query.limit(limit)

        return query.sql(dialect="mysql")

    @staticmethod
    def _build_allowed_fields(plan: PlanDSL, evidence: EvidenceBundle) -> set:
        allowed = {f"{s.table}.{s.field}" for s in evidence.schema_candidates}
        metric_def = next((m for m in evidence.metric_candidates if m.metric_id == plan.metric_id), None)
        if metric_def:
            for field in metric_def.required_fields:
                if "." in field:
                    allowed.add(field)
        join_path = next((j for j in evidence.join_paths if j.join_path_id == plan.join_path_id), None)
        if join_path:
            for edge in join_path.edges:
                allowed.add(f"{edge.left_table}.{edge.left_field}")
                allowed.add(f"{edge.right_table}.{edge.right_field}")
        return allowed

    @staticmethod
    def _get_metric_def(metric_id: str, evidence: EvidenceBundle) -> MetricDef:
        for metric in evidence.metric_candidates:
            if metric.metric_id == metric_id:
                return metric
        raise ValueError("metric_id not found in evidence")

    @staticmethod
    def _pick_time_field(evidence: EvidenceBundle, metric_def: MetricDef) -> Tuple[str, str]:
        for item in evidence.schema_candidates:
            if item.field.lower() in {"ts", "timestamp", "event_time", "date", "dt"}:
                return item.table, item.field
            if item.data_type.lower() in {"datetime", "timestamp", "date"}:
                return item.table, item.field
        for field in metric_def.required_fields:
            if field.endswith(".ts") or field.endswith(".date"):
                table, fld = field.split(".", 1)
                return table, fld
        raise ValueError("No time field found for time_range")

    @staticmethod
    def _pick_base_table(
        plan: PlanDSL,
        evidence: EvidenceBundle,
        metric_def: MetricDef,
        time_table: str,
    ) -> str:
        join_path = next((j for j in evidence.join_paths if j.join_path_id == plan.join_path_id), None)
        if join_path and join_path.edges:
            return join_path.edges[0].left_table
        if plan.dimensions:
            return plan.dimensions[0].table
        if metric_def.required_fields:
            if "." in metric_def.required_fields[0]:
                return metric_def.required_fields[0].split(".", 1)[0]
        return time_table

    @staticmethod
    def _time_bucket_expr(table: str, field: str, grain: str) -> exp.Expression:
        col_ref = f"{table}.{field}"
        if grain == "15m":
            expr = f"FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP({col_ref})/900)*900)"
        elif grain == "hour":
            expr = f"DATE_FORMAT({col_ref}, '%Y-%m-%d %H:00:00')"
        elif grain == "day":
            expr = f"DATE_FORMAT({col_ref}, '%Y-%m-%d')"
        elif grain == "week":
            expr = f"YEARWEEK({col_ref}, 1)"
        elif grain == "month":
            expr = f"DATE_FORMAT({col_ref}, '%Y-%m')"
        else:
            raise ValueError("Unsupported time_grain")
        return parse_one(expr, dialect="mysql")

    @staticmethod
    def _metric_expr(metric_def: MetricDef) -> exp.Expression:
        fields = metric_def.required_fields
        if not fields:
            raise ValueError("metric required_fields empty")
        if len(fields) == 1:
            table, field = fields[0].split(".", 1)
            return exp.func("SUM", exp.column(field, table=table))
        table_a, field_a = fields[0].split(".", 1)
        table_b, field_b = fields[1].split(".", 1)
        sum_a = exp.func("SUM", exp.column(field_a, table=table_a))
        sum_b = exp.func("SUM", exp.column(field_b, table=table_b))
        return exp.Div(this=sum_a, expression=exp.NullIf(this=sum_b, expression=exp.Literal.number(0)))

    @staticmethod
    def _time_range_filter(table: str, field: str, plan: PlanDSL) -> exp.Expression:
        col_expr = exp.column(field, table=table)
        return exp.Between(
            this=col_expr,
            low=exp.Literal.string(plan.time_range.start),
            high=exp.Literal.string(plan.time_range.end),
        )

    @staticmethod
    def _filter_expr(fil) -> exp.Expression:
        col_expr = exp.column(fil.field, table=fil.table)
        op = fil.op
        value = fil.value
        if op == "=":
            return exp.EQ(this=col_expr, expression=SqlCompiler._literal(value))
        if op == "!=":
            return exp.NEQ(this=col_expr, expression=SqlCompiler._literal(value))
        if op == ">":
            return exp.GT(this=col_expr, expression=SqlCompiler._literal(value))
        if op == ">=":
            return exp.GTE(this=col_expr, expression=SqlCompiler._literal(value))
        if op == "<":
            return exp.LT(this=col_expr, expression=SqlCompiler._literal(value))
        if op == "<=":
            return exp.LTE(this=col_expr, expression=SqlCompiler._literal(value))
        if op == "like":
            return exp.Like(this=col_expr, expression=SqlCompiler._literal(value))
        if op == "in":
            if not isinstance(value, list):
                raise ValueError("IN operator requires list value")
            return exp.In(this=col_expr, expressions=[SqlCompiler._literal(v) for v in value])
        if op == "between":
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("BETWEEN operator requires two values")
            return exp.Between(
                this=col_expr,
                low=SqlCompiler._literal(value[0]),
                high=SqlCompiler._literal(value[1]),
            )
        raise ValueError("Unsupported filter op")

    @staticmethod
    def _literal(value) -> exp.Expression:
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        return exp.Literal.string(str(value))

    @staticmethod
    def _and(left: Optional[exp.Expression], right: exp.Expression) -> exp.Expression:
        if left is None:
            return right
        return exp.and_(left, right)

    @staticmethod
    def _order_expr(plan: PlanDSL, allowed_fields: set) -> Optional[exp.Expression]:
        by = plan.sort.by
        if by in {"metric", plan.metric_id}:
            return exp.Ordered(this=exp.column(plan.metric_id), desc=plan.sort.order == "desc")
        if by in {"time", "time_bucket"}:
            if plan.intent != "trend":
                return None
            return exp.Ordered(this=exp.column("time_bucket"), desc=plan.sort.order == "desc")
        if "." in by:
            if by not in allowed_fields:
                raise ValueError(f"Sort field not allowed: {by}")
            table, field = by.split(".", 1)
            return exp.Ordered(
                this=exp.column(field, table=table), desc=plan.sort.order == "desc"
            )
        if any(field.endswith(f".{by}") for field in allowed_fields):
            return exp.Ordered(this=exp.column(by), desc=plan.sort.order == "desc")
        raise ValueError(f"Sort field not allowed: {by}")

    @staticmethod
    def _get_join_path(join_path_id: str, evidence: EvidenceBundle):
        if join_path_id == "NONE":
            return None
        for jp in evidence.join_paths:
            if jp.join_path_id == join_path_id:
                return jp
        return None
