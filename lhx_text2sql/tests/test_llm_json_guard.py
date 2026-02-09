import pytest

from app.core.config import get_settings
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


def test_llm_non_json_output_rejected():
    settings = get_settings()
    schema = load_schema(settings.schema_path)

    schema_kb = SchemaKB(settings.schema_kb_path, SimpleInMemoryVectorStore())
    join_kb = JoinGraphKB(settings.join_kb_path, SimpleInMemoryVectorStore())
    metric_kb = MetricKB(settings.metric_kb_path, SimpleInMemoryVectorStore())
    template_kb = TemplateKB(settings.template_kb_path, SimpleInMemoryVectorStore())

    llm_client = MockLLMClient(force_invalid=True, force_sql=True)
    validator = PlanValidator(schema)
    repairer = PlanRepair(llm_client, schema, prompt_path=f"{settings.prompt_dir}/plan_repair.txt")

    planner = Planner(
        settings=settings,
        llm_client=llm_client,
        schema_kb=schema_kb,
        join_kb=join_kb,
        metric_kb=metric_kb,
        template_kb=template_kb,
        validator=validator,
        repairer=repairer,
        prompt_path=f"{settings.prompt_dir}/plan_generate.txt",
    )

    with pytest.raises(ValueError):
        planner.generate_plan("test question", {"role": "analyst"}, None)
