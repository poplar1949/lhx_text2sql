import pytest

from app.core.compile.compiler import SqlCompiler
from app.core.models import EvidenceBundle, MetricDef, PlanDSL, SchemaEntity, TemplateRule, Dimension, OutputSpec, TimeRange


def test_compiler_guard_rejects_unauthorized_field():
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
                field="feeder_id",
                field_desc="",
                aliases=[],
                unit="",
                data_type="string",
                quality_tags=[],
            )
        ],
        join_paths=[],
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

    plan = PlanDSL(
        version="1.0",
        intent="trend",
        metric_id="load_rate",
        metric_params={},
        dimensions=[Dimension(table="feeder", field="bad_field")],
        time_range=TimeRange(start="2024-01-01", end="2024-01-31"),
        time_grain="day",
        filters=[],
        join_path_id="NONE",
        sort=None,
        limit=10,
        output=OutputSpec(format="table", chart_suggest="line"),
        confidence=0.5,
        clarifications=[],
    )

    compiler = SqlCompiler()
    with pytest.raises(ValueError):
        compiler.compile(plan, evidence)
