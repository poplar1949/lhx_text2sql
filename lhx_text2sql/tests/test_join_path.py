from app.core.models import EvidenceBundle, MetricDef, SchemaEntity, TemplateRule, JoinPath, JoinEdge
from app.core.planning.validator import PlanValidator
from app.core.schema import load_schema


def test_join_path_not_found():
    schema = load_schema("schemas/plan_dsl.schema.json")
    validator = PlanValidator(schema)
    evidence = EvidenceBundle(
        metric_candidates=[
            MetricDef(
                metric_id="load_rate",
                name="Load rate",
                definition="",
                formula="",
                required_fields=["feeder.load_kw", "feeder.capacity_kw"],
                default_time_grain="day",
                unit="ratio",
            )
        ],
        schema_candidates=[
            SchemaEntity(
                table="feeder",
                field="ts",
                field_desc="",
                aliases=[],
                unit="",
                data_type="datetime",
                quality_tags=[],
            )
        ],
        join_paths=[
            JoinPath(
                join_path_id="valid_path",
                description="",
                tables=["feeder"],
                edges=[
                    JoinEdge(
                        left_table="feeder",
                        left_field="feeder_id",
                        right_table="transformer",
                        right_field="feeder_id",
                        join_type="inner",
                    )
                ],
            )
        ],
        template_rules=[
            TemplateRule(
                template_id="trend_template",
                intent="trend",
                allowed_aggs=["sum"],
                allowed_funcs=[],
                required_clauses=["time_range"],
            )
        ],
    )

    plan = {
        "version": "1.0",
        "intent": "trend",
        "metric_id": "load_rate",
        "metric_params": {},
        "dimensions": [],
        "time_range": {"start": "2024-01-01", "end": "2024-01-31"},
        "time_grain": "day",
        "filters": [],
        "join_path_id": "missing_path",
        "output": {"format": "table", "chart_suggest": "line"},
        "confidence": 0.5,
        "clarifications": [],
    }

    errors = validator.validate(plan, evidence)
    assert any(e.code == "join_path_not_found" for e in errors)
