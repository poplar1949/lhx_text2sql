import time
from dataclasses import dataclass
from typing import List

import pymysql

from app.core.config import Settings
from app.core.execute.quality import run_quality_checks
from app.core.models import DataPreview, EvidenceBundle, MetricDef, PlanDSL


@dataclass
class ExecutionResult:
    data_preview: DataPreview
    quality_warnings: List[str]


class QueryExecutor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute(self, sql: str, plan: PlanDSL, evidence: EvidenceBundle) -> ExecutionResult:
        issues = self.estimate_cost(plan)
        if issues:
            raise ValueError("; ".join(issues))

        metric_def = self._find_metric_def(plan.metric_id, evidence)

        if self.settings.use_mock_db:
            data_preview = self._mock_preview(plan)
            warnings = run_quality_checks(metric_def, data_preview)
            return ExecutionResult(data_preview=data_preview, quality_warnings=warnings)

        start_time = time.time()
        conn = pymysql.connect(
            host=self.settings.mysql_host,
            port=self.settings.mysql_port,
            user=self.settings.mysql_user,
            password=self.settings.mysql_password,
            database=self.settings.mysql_database,
            charset=self.settings.mysql_charset,
            connect_timeout=self.settings.mysql_connect_timeout,
            read_timeout=self.settings.mysql_read_timeout,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchmany(20)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
        finally:
            conn.close()

        data_preview = DataPreview(columns=columns, rows=[list(row) for row in rows])
        warnings = run_quality_checks(metric_def, data_preview)
        return ExecutionResult(data_preview=data_preview, quality_warnings=warnings)

    @staticmethod
    def estimate_cost(plan: PlanDSL) -> List[str]:
        issues: List[str] = []
        if not plan.time_range:
            issues.append("Missing time_range, query rejected.")
        if plan.limit and plan.limit > 10000:
            issues.append("Limit too large, query rejected.")
        return issues

    @staticmethod
    def _find_metric_def(metric_id: str, evidence: EvidenceBundle) -> MetricDef:
        for metric in evidence.metric_candidates:
            if metric.metric_id == metric_id:
                return metric
        return MetricDef(
            metric_id=metric_id,
            name=metric_id,
            definition="",
            formula="",
            required_fields=[],
            default_time_grain="day",
            unit="",
        )

    @staticmethod
    def _mock_preview(plan: PlanDSL) -> DataPreview:
        columns: List[str] = []
        if plan.intent == "trend":
            columns.append("time_bucket")
        for dim in plan.dimensions:
            columns.append(dim.field)
        columns.append(plan.metric_id)

        if plan.intent == "trend":
            dim_values = ["sample"] * len(plan.dimensions)
            rows = [
                [plan.time_range.start] + dim_values + [0.05],
                [plan.time_range.end] + dim_values + [0.06],
            ]
        elif plan.intent == "rank":
            dim_values = ["sample"] * len(plan.dimensions)
            rows = [
                dim_values + [0.12],
                dim_values + [0.11],
            ]
        else:
            rows = [["sample"] * len(plan.dimensions) + [0.08]]

        return DataPreview(columns=columns, rows=rows)
