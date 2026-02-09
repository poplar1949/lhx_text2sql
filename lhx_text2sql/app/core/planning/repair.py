import json
from typing import Dict, List

from app.core.llm.client import LLMClient


class PlanRepair:
    def __init__(self, llm_client: LLMClient, schema: dict, prompt_path: str) -> None:
        self.llm_client = llm_client
        self.schema = schema
        self.prompt_template = self._load_prompt(prompt_path)

    @staticmethod
    def _load_prompt(path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def repair(self, original_plan: Dict, validation_errors: List[Dict], evidence) -> Dict:
        payload = {
            "original_plan": original_plan,
            "validation_errors": validation_errors,
            "evidence": evidence.dict(),
            "schema": self.schema,
        }
        prompt = f"{self.prompt_template}\n\n<INPUTS>\n{json.dumps(payload, ensure_ascii=False)}"
        plan = self.llm_client.generate_json(prompt=prompt, schema=self.schema)
        if not isinstance(plan, dict):
            raise ValueError("LLM repair output is not JSON")
        return plan
