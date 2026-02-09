import time
from typing import Any, Dict, Optional

from app.core.audit.audit_log import AuditLogger
from app.core.compile.compiler import SqlCompiler
from app.core.config import get_settings
from app.core.execute.answer import AnswerGenerator
from app.core.execute.executor import QueryExecutor
from app.core.llm.client import RealLLMClient
from app.core.llm.mock_client import MockLLMClient
from app.core.planning.planner import Planner
from app.core.planning.repair import PlanRepair
from app.core.planning.validator import PlanValidator
from app.core.rag.faiss_store import SimpleInMemoryVectorStore
from app.core.rag.kb_join import JoinGraphKB
from app.core.rag.kb_metric import MetricKB
from app.core.rag.kb_schema import SchemaKB
from app.core.rag.kb_template import TemplateKB
from app.core.schema import load_schema


class Text2SQLEngine:
    def __init__(self) -> None:
        self.settings = get_settings()
        schema = load_schema(self.settings.schema_path)

        self.schema_kb = SchemaKB(self.settings.schema_kb_path, SimpleInMemoryVectorStore())
        self.join_kb = JoinGraphKB(self.settings.join_kb_path, SimpleInMemoryVectorStore())
        self.metric_kb = MetricKB(self.settings.metric_kb_path, SimpleInMemoryVectorStore())
        self.template_kb = TemplateKB(self.settings.template_kb_path, SimpleInMemoryVectorStore())

        self.llm_client = self._init_llm_client()
        self.validator = PlanValidator(schema)
        self.repairer = PlanRepair(
            self.llm_client, schema, prompt_path=f"{self.settings.prompt_dir}/plan_repair.txt"
        )
        self.planner = Planner(
            settings=self.settings,
            llm_client=self.llm_client,
            schema_kb=self.schema_kb,
            join_kb=self.join_kb,
            metric_kb=self.metric_kb,
            template_kb=self.template_kb,
            validator=self.validator,
            repairer=self.repairer,
            prompt_path=f"{self.settings.prompt_dir}/plan_generate.txt",
        )

        self.compiler = SqlCompiler()
        self.executor = QueryExecutor(self.settings)
        self.answer_generator = AnswerGenerator(
            self.llm_client, prompt_path=f"{self.settings.prompt_dir}/answer_generate.txt"
        )
        self.audit_logger = AuditLogger(self.settings.audit_log_path)

    def _init_llm_client(self):
        if self.settings.llm_mode == "mock":
            return MockLLMClient()
        if self.settings.llm_mode == "no_llm":
            return MockLLMClient()
        try:
            return RealLLMClient()
        except ValueError:
            self.settings.llm_mode = "mock"
            return MockLLMClient()

    def run_query(
        self,
        question: str,
        user_context: Optional[Dict[str, Any]] = None,
        time_range: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        user_context = user_context or {}
        audit_id = self.audit_logger.new_id()
        start_time = time.time()
        stage = "init"
        try:
            stage = "plan"
            plan_result = self.planner.generate_plan(
                question=question,
                user_context=user_context,
                time_range=time_range,
            )
            stage = "compile"
            sql = self.compiler.compile(plan_result.plan, plan_result.evidence)
            stage = "execute"
            exec_result = self.executor.execute(sql, plan_result.plan, plan_result.evidence)
            stage = "answer"
            answer_text = self.answer_generator.generate(
                question=question,
                plan_dsl=plan_result.plan,
                sql=sql,
                metric_def=plan_result.metric_def,
                data_preview=exec_result.data_preview,
                quality_warnings=exec_result.quality_warnings,
            )
            elapsed_ms = int((time.time() - start_time) * 1000)
            self.audit_logger.write(
                audit_id=audit_id,
                question=question,
                user_context=user_context,
                evidence_summary=plan_result.evidence_summary,
                plan_initial=plan_result.plan_initial,
                plan_final=plan_result.plan.dict(),
                validation_errors=[e.dict() for e in plan_result.validation_errors],
                sql=sql,
                elapsed_ms=elapsed_ms,
                error=None,
            )
            return {
                "audit_log_id": audit_id,
                "plan_dsl": plan_result.plan.dict(),
                "sql": sql,
                "data_preview": exec_result.data_preview.dict(),
                "answer_text": answer_text,
                "debug": {
                    "evidence_summary": plan_result.evidence_summary,
                    "validation_errors": [e.dict() for e in plan_result.validation_errors],
                },
            }
        except Exception as exc:
            elapsed_ms = int((time.time() - start_time) * 1000)
            error_text = f"[{stage}] {exc}"
            self.audit_logger.write(
                audit_id=audit_id,
                question=question,
                user_context=user_context,
                evidence_summary="",
                plan_initial=None,
                plan_final=None,
                validation_errors=[],
                sql=None,
                elapsed_ms=elapsed_ms,
                error=error_text,
            )
            raise ValueError(error_text) from exc

    def test_connections(self) -> Dict[str, str]:
        results: Dict[str, str] = {}
        if self.settings.llm_mode == "mock":
            results["llm"] = "mock"
        else:
            try:
                client = RealLLMClient()
                _ = client.generate_text("ping")
                results["llm"] = "ok"
            except Exception as exc:
                results["llm"] = f"error: {exc}"

        if self.settings.use_mock_db:
            results["mysql"] = "mock"
        else:
            try:
                import pymysql

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
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                finally:
                    conn.close()
                results["mysql"] = "ok"
            except Exception as exc:
                results["mysql"] = f"error: {exc}"
        return results
