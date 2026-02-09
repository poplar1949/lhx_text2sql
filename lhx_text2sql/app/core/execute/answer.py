import json
from typing import List

from app.core.models import DataPreview, MetricDef, PlanDSL
from app.core.llm.client import LLMClient


class AnswerGenerator:
    def __init__(self, llm_client: LLMClient, prompt_path: str) -> None:
        self.llm_client = llm_client
        self.prompt_template = ""
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                self.prompt_template = f.read()
        except FileNotFoundError:
            self.prompt_template = ""

    def generate(
        self,
        question: str,
        plan_dsl: PlanDSL,
        sql: str,
        metric_def: MetricDef,
        data_preview: DataPreview,
        quality_warnings: List[str],
    ) -> str:
        if self._can_use_llm():
            payload = {
                "question": question,
                "plan_dsl": plan_dsl.dict(),
                "sql": sql,
                "metric_definition": metric_def.dict(),
                "result_preview": data_preview.dict(),
            }
            prompt = f"{self.prompt_template}\n\n{json.dumps(payload, ensure_ascii=False, default=str)}"
            try:
                return self.llm_client.generate_text(prompt)
            except NotImplementedError:
                pass
        return self._rule_based(plan_dsl, metric_def, data_preview, quality_warnings)

    def _rule_based(
        self,
        plan_dsl: PlanDSL,
        metric_def: MetricDef,
        data_preview: DataPreview,
        quality_warnings: List[str],
    ) -> str:
        if not data_preview.rows:
            return (
                f"结果为空。可能原因：时间范围 {plan_dsl.time_range.start} 至 {plan_dsl.time_range.end} 内无数据，"
                "或筛选条件过窄，或存在数据质量问题。"
                "建议调整时间范围或减少过滤条件后重试。"
            )

        metric_value = self._extract_metric_value(plan_dsl.metric_id, data_preview)
        conclusion = "暂无" if metric_value is None else f"约为 {metric_value}"
        warnings = "" if not quality_warnings else f"注意：{'；'.join(quality_warnings)}"

        return (
            f"指标口径：{metric_def.definition}（单位：{metric_def.unit}）。"
            f"时间范围：{plan_dsl.time_range.start} 至 {plan_dsl.time_range.end}。"
            f"主要结论：1) {metric_def.name} {conclusion}。"
            f"可视化建议：{plan_dsl.output.chart_suggest}。"
            f"{warnings}"
        )

    @staticmethod
    def _extract_metric_value(metric_id: str, data_preview: DataPreview):
        if metric_id not in data_preview.columns:
            return None
        idx = data_preview.columns.index(metric_id)
        values = [row[idx] for row in data_preview.rows if isinstance(row[idx], (int, float))]
        if not values:
            return None
        return round(sum(values) / len(values), 4)

    def _can_use_llm(self) -> bool:
        return self.llm_client.__class__.__name__ not in {"MockLLMClient"}
