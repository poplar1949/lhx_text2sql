from typing import List

from app.core.models import DataPreview, MetricDef


def run_quality_checks(metric_def: MetricDef, data_preview: DataPreview) -> List[str]:
    warnings: List[str] = []
    if not data_preview.rows:
        warnings.append("结果为空，可能是时间范围或过滤条件过窄，或存在数据质量问题。")
        return warnings

    metric_col = metric_def.metric_id
    if metric_col in data_preview.columns:
        idx = data_preview.columns.index(metric_col)
        values = [row[idx] for row in data_preview.rows if isinstance(row[idx], (int, float))]
        if values:
            min_val, max_val = min(values), max(values)
            if metric_def.unit in {"%", "ratio"} and (min_val < 0 or max_val > 1.5):
                warnings.append("指标值超出常见范围，建议检查口径或数据质量。")
            if metric_def.unit in {"count", "min"} and min_val < 0:
                warnings.append("指标值出现负数，建议检查数据质量。")

    if "unit" in data_preview.columns:
        idx = data_preview.columns.index("unit")
        units = {row[idx] for row in data_preview.rows if row[idx] is not None}
        if len(units) > 1:
            warnings.append("结果中的单位不一致，请核对量纲。")

    return warnings
