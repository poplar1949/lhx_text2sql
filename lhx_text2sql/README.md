# Power Text2SQL (LLM + RAG + Query Plan DSL)

This MVP implements a power-domain Text2SQL system where the LLM only outputs a JSON Query Plan DSL and SQL is compiled by a guarded compiler. It uses RAG knowledge bases (schema/join/metric/template), validation + repair, audit logging, safe execution, and result quality checks.

## Requirements
- Python 3.10+
- MySQL 8.x for production execution

## Install
```bash
pip install -r requirements.txt
```

## Run (GUI only)
Launch the desktop UI (no HTTP server needed):
```bash
python -m app.gui
```

## MySQL config
By default, `TEXT2SQL_USE_MOCK_DB=true` so the API returns a demo response without a real database. To use MySQL:
```bash
set TEXT2SQL_USE_MOCK_DB=false
set TEXT2SQL_MYSQL_HOST=localhost
set TEXT2SQL_MYSQL_PORT=3306
set TEXT2SQL_MYSQL_USER=root
set TEXT2SQL_MYSQL_PASSWORD=your_password
set TEXT2SQL_MYSQL_DATABASE=power
```

## LLM (SiliconFlow / DeepSeek)
Set the LLM client to real mode and provide SiliconFlow credentials.

### Option A: .env file (recommended)
Create a `.env` file (you can copy `.env.example`) and fill in your values:
```bash
copy .env.example .env
```
Then edit `.env`:
```
TEXT2SQL_LLM_MODE=real
TEXT2SQL_LLM_BASE_URL=https://api.siliconflow.cn/v1
TEXT2SQL_LLM_API_KEY=your_api_key
TEXT2SQL_LLM_MODEL=your_deepseek_model_name
```

### Option B: set env vars in terminal
```bash
set TEXT2SQL_LLM_MODE=real
set TEXT2SQL_LLM_BASE_URL=https://api.siliconflow.cn/v1
set TEXT2SQL_LLM_API_KEY=your_api_key
set TEXT2SQL_LLM_MODEL=deepseek-model-name
```
Replace `deepseek-model-name` with the model name provided by SiliconFlow (e.g., a DeepSeek model).

## Knowledge bases
- `data/schema_kb.json`: schema semantics
- `data/join_kb.json`: join paths
- `data/metric_kb.json`: metrics with definitions and required fields
- `data/template_kb.json`: template rules and constraints

## Response fields
- `plan_dsl`: validated Query Plan DSL (JSON)
- `sql`: compiled MySQL SQL (sqlglot)
- `data_preview`: up to 20 rows
- `answer_text`: natural language summary (includes metric definition + unit + time range)
- `debug`: evidence summary + validation errors

## Project layout
```
app/
  core/
    config.py
    models.py
    schema.py
    rag/
    planning/
    compile/
    execute/
    llm/
    audit/
prompts/
  plan_generate.txt
  plan_repair.txt
  answer_generate.txt
schemas/
  plan_dsl.schema.json
data/
  schema_kb.json
  join_kb.json
  metric_kb.json
  template_kb.json
tests/
```
