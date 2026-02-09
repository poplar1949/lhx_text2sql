from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Power Text2SQL"
    prompt_dir: str = "prompts"
    schema_path: str = "schemas/plan_dsl.schema.json"
    schema_kb_path: str = "data/schema_kb.json"
    join_kb_path: str = "data/join_kb.json"
    metric_kb_path: str = "data/metric_kb.json"
    template_kb_path: str = "data/template_kb.json"
    audit_log_path: str = "data/audit_logs.jsonl"

    llm_mode: str = "mock"
    llm_base_url: str = "https://api.siliconflow.cn/v1"
    llm_api_key: str = ""
    llm_model: str = ""
    llm_timeout: int = 30
    llm_max_retries: int = 2
    llm_force_json: bool = True
    llm_extract_json: bool = True
    llm_plan_trim_top_k: int = 2
    llm_plan_retry_on_timeout: bool = True
    use_mock_db: bool = True
    fixed_metric_id: str = ""

    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "root"
    mysql_database: str = "power"
    mysql_charset: str = "utf8mb4"
    mysql_connect_timeout: int = 5
    mysql_read_timeout: int = 30

    rag_top_k: int = 5
    rag_top_k_second: int = 8

    class Config:
        env_prefix = "TEXT2SQL_"
        case_sensitive = False
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
