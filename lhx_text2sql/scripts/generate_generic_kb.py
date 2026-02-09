import json
from pathlib import Path


NUMERIC_TYPES = {
    "int",
    "bigint",
    "smallint",
    "mediumint",
    "tinyint",
    "decimal",
    "float",
    "double",
    "numeric",
}


def _load_schema(path: str):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data


def _build_metric_name(prefix: str, field_desc: str, field: str) -> str:
    label = field_desc.strip() or field
    return f"{prefix}_{label}"


def build_metrics(schema_items):
    metrics = []
    for item in schema_items:
        data_type = (item.get("data_type") or "").lower()
        if data_type not in NUMERIC_TYPES:
            continue
        table = item["table"]
        field = item["field"]
        field_desc = item.get("field_desc", "")
        unit = item.get("unit", "")
        for agg in ["sum", "avg", "max", "min"]:
            metric_id = f"{agg}_{table}_{field}"
            name = _build_metric_name(agg, field_desc, field)
            formula = f"{agg.upper()}({field})"
            metrics.append(
                {
                    "metric_id": metric_id,
                    "name": name,
                    "definition": f"{agg.upper()} of {table}.{field}",
                    "formula": formula,
                    "required_fields": [f"{table}.{field}"],
                    "default_time_grain": "day",
                    "unit": unit,
                }
            )
    return metrics


def build_templates():
    return [
        {
            "template_id": "trend_template",
            "intent": "trend",
            "allowed_aggs": ["sum", "avg", "max", "min"],
            "allowed_funcs": ["date_format", "yearweek", "from_unixtime", "unix_timestamp"],
            "required_clauses": ["time_range", "time_grain", "group_by_time"],
        },
        {
            "template_id": "rank_template",
            "intent": "rank",
            "allowed_aggs": ["sum", "avg", "max", "min"],
            "allowed_funcs": [],
            "required_clauses": ["order_by", "limit"],
        },
        {
            "template_id": "aggregate_template",
            "intent": "aggregate",
            "allowed_aggs": ["sum", "avg", "max", "min"],
            "allowed_funcs": [],
            "required_clauses": ["time_range"],
        },
        {
            "template_id": "compare_template",
            "intent": "compare",
            "allowed_aggs": ["sum", "avg", "max", "min"],
            "allowed_funcs": ["date_format", "yearweek", "from_unixtime", "unix_timestamp"],
            "required_clauses": ["time_range"],
        },
    ]


def main() -> None:
    schema_items = _load_schema("data/schema_kb.json")
    metrics = build_metrics(schema_items)
    templates = build_templates()

    Path("data/metric_kb.json").write_text(
        json.dumps(metrics, ensure_ascii=True, indent=2), encoding="utf-8"
    )
    Path("data/template_kb.json").write_text(
        json.dumps(templates, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    print("Updated:")
    print("- data/metric_kb.json (generic metrics)")
    print("- data/template_kb.json (generic templates)")


if __name__ == "__main__":
    main()
