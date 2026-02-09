from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field, conint, confloat


class Dimension(BaseModel):
    table: str
    field: str

    class Config:
        extra = "forbid"


class Filter(BaseModel):
    table: str
    field: str
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "like", "between"]
    value: Any

    class Config:
        extra = "forbid"


class SortSpec(BaseModel):
    by: str
    order: Literal["asc", "desc"]

    class Config:
        extra = "forbid"


class OutputSpec(BaseModel):
    format: Literal["table", "single_value"]
    chart_suggest: Literal["line", "bar", "heatmap", "none"]

    class Config:
        extra = "forbid"


class TimeRange(BaseModel):
    start: str
    end: str

    class Config:
        extra = "forbid"


class PlanDSL(BaseModel):
    version: Literal["1.0"]
    intent: Literal["trend", "aggregate", "rank", "compare", "detail"]
    metric_id: str
    metric_params: Dict[str, Any] = Field(default_factory=dict)
    dimensions: List[Dimension]
    time_range: TimeRange
    time_grain: Literal["15m", "hour", "day", "week", "month"]
    filters: List[Filter]
    join_path_id: str
    sort: Optional[SortSpec] = None
    limit: Optional[conint(ge=1, le=10000)] = None
    output: OutputSpec
    confidence: confloat(ge=0, le=1)
    clarifications: List[str]
    errors_unresolved: Optional[List[str]] = None

    class Config:
        extra = "forbid"


class MetricDef(BaseModel):
    metric_id: str
    name: str
    definition: str
    formula: str
    required_fields: List[str]
    default_time_grain: str
    unit: str

    class Config:
        extra = "forbid"


class SchemaEntity(BaseModel):
    table: str
    field: str
    field_desc: str
    aliases: List[str]
    unit: str
    data_type: str
    quality_tags: List[str]

    class Config:
        extra = "forbid"


class JoinEdge(BaseModel):
    left_table: str
    left_field: str
    right_table: str
    right_field: str
    join_type: str

    class Config:
        extra = "forbid"


class JoinPath(BaseModel):
    join_path_id: str
    description: str
    tables: List[str]
    edges: List[JoinEdge]

    class Config:
        extra = "forbid"


class TemplateRule(BaseModel):
    template_id: str
    intent: str
    allowed_aggs: List[str]
    allowed_funcs: List[str]
    required_clauses: List[str]

    class Config:
        extra = "forbid"


class EvidenceBundle(BaseModel):
    metric_candidates: List[MetricDef]
    schema_candidates: List[SchemaEntity]
    join_paths: List[JoinPath]
    template_rules: List[TemplateRule]

    class Config:
        extra = "forbid"


class ValidationError(BaseModel):
    code: str
    message: str
    field_path: str
    suggestions: List[str] = Field(default_factory=list)

    class Config:
        extra = "forbid"


class DataPreview(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

    class Config:
        extra = "forbid"


class DebugInfo(BaseModel):
    evidence_summary: str
    validation_errors: List[ValidationError] = Field(default_factory=list)

    class Config:
        extra = "forbid"


class QueryRequest(BaseModel):
    question: str
    user_context: Dict[str, Any] = Field(default_factory=dict)
    time_range: Optional[TimeRange] = None

    class Config:
        extra = "forbid"


class QueryResponse(BaseModel):
    audit_log_id: str
    plan_dsl: Dict[str, Any]
    sql: str
    data_preview: DataPreview
    answer_text: str
    debug: DebugInfo

    class Config:
        extra = "forbid"
