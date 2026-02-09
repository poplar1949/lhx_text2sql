import json
from abc import ABC, abstractmethod
from typing import Dict
from urllib import request
from urllib.error import HTTPError, URLError

from app.core.config import get_settings


class LLMClient(ABC):
    @abstractmethod
    def generate_json(self, prompt: str, schema: dict) -> Dict:
        raise NotImplementedError

    def generate_text(self, prompt: str) -> str:
        raise NotImplementedError


class RealLLMClient(LLMClient):
    def __init__(self, *args, **kwargs) -> None:
        settings = get_settings()
        self.base_url = getattr(settings, "llm_base_url", "").rstrip("/")
        self.api_key = getattr(settings, "llm_api_key", "")
        self.model = getattr(settings, "llm_model", "")
        self.timeout = getattr(settings, "llm_timeout", 30)
        self.max_retries = max(1, int(getattr(settings, "llm_max_retries", 1)))
        self.force_json = bool(getattr(settings, "llm_force_json", True))
        self.extract_json = bool(getattr(settings, "llm_extract_json", True))
        if not self.base_url:
            raise ValueError("LLM base_url is empty")
        if not self.api_key:
            raise ValueError("LLM api_key is empty")
        if not self.model:
            raise ValueError("LLM model is empty")

    def generate_json(self, prompt: str, schema: dict) -> Dict:
        last_error: Exception | None = None
        for _ in range(self.max_retries):
            content = self._chat(prompt, require_json=True)
            try:
                return json.loads(content)
            except json.JSONDecodeError as exc:
                if self.extract_json:
                    extracted = self._extract_json_object(content)
                    if extracted is not None:
                        try:
                            return json.loads(extracted)
                        except json.JSONDecodeError as nested_exc:
                            last_error = nested_exc
                            continue
                last_error = exc
                continue
        raise ValueError(f"LLM output is not valid JSON: {last_error}") from last_error

    def generate_text(self, prompt: str) -> str:
        return self._chat(prompt, require_json=False)

    def _chat(self, prompt: str, require_json: bool) -> str:
        url = f"{self.base_url}/chat/completions"
        messages = []
        if require_json and self.force_json:
            messages.append(
                {
                    "role": "system",
                    "content": "You must output a single JSON object and nothing else.",
                }
            )
        messages.append({"role": "user", "content": prompt})
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
        }
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        req = request.Request(url, data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
        except (HTTPError, URLError) as exc:
            raise ValueError(f"LLM request failed: {exc}") from exc
        try:
            resp_json = json.loads(body)
            return resp_json["choices"][0]["message"]["content"]
        except (KeyError, IndexError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid LLM response format") from exc

    @staticmethod
    def _extract_json_object(text: str) -> str | None:
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for idx in range(start, len(text)):
            ch = text[idx]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
        return None
