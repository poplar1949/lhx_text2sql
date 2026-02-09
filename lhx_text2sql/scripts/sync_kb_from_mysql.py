import json
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import pymysql
from dotenv import load_dotenv


TIME_FIELD_NAMES = {
    "ts",
    "timestamp",
    "event_time",
    "date",
    "dt",
    "created_at",
    "updated_at",
}
NUMERIC_TYPES = {
    "int",
    "bigint",
    "smallint",
    "mediumint",
    "tinyint",
    "decimal",
    "float",
    "double",
}


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _connect() -> pymysql.connections.Connection:
    host = _get_env("TEXT2SQL_MYSQL_HOST", "127.0.0.1")
    port = int(_get_env("TEXT2SQL_MYSQL_PORT", "3306"))
    user = _get_env("TEXT2SQL_MYSQL_USER", "root")
    password = _get_env("TEXT2SQL_MYSQL_PASSWORD", "")
    database = _get_env("TEXT2SQL_MYSQL_DATABASE", "")
    if not database:
        raise ValueError("TEXT2SQL_MYSQL_DATABASE is required")
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=10,
    )


def load_columns(conn: pymysql.connections.Connection, schema: str) -> List[Dict]:
    sql = """
        SELECT table_name,
               column_name,
               data_type,
               column_comment,
               column_key
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        rows = cur.fetchall()
    columns = []
    for row in rows:
        table, column, data_type, comment, column_key = row
        columns.append(
            {
                "table": table,
                "field": column,
                "data_type": data_type or "",
                "field_desc": comment or "",
                "column_key": column_key or "",
            }
        )
    return columns


def load_foreign_keys(conn: pymysql.connections.Connection, schema: str) -> List[Dict]:
    sql = """
        SELECT table_name,
               column_name,
               referenced_table_name,
               referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s
          AND referenced_table_name IS NOT NULL
          AND referenced_column_name IS NOT NULL
        ORDER BY table_name, column_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        rows = cur.fetchall()
    fks = []
    for row in rows:
        table, column, ref_table, ref_column = row
        fks.append(
            {
                "table": table,
                "column": column,
                "ref_table": ref_table,
                "ref_column": ref_column,
            }
        )
    return fks


def build_schema_kb(columns: List[Dict], fks: List[Dict]) -> List[Dict]:
    fk_set = {(fk["table"], fk["column"]) for fk in fks}
    items = []
    for col in columns:
        data_type = (col.get("data_type") or "").lower()
        field = col["field"]
        tags = []
        if col.get("column_key") == "PRI":
            tags.append("primary_key")
        if (col["table"], field) in fk_set:
            tags.append("foreign_key")
        if field.lower() in TIME_FIELD_NAMES or data_type in {"date", "datetime", "timestamp"}:
            tags.append("time")
        if data_type in NUMERIC_TYPES:
            tags.append("metric")
        items.append(
            {
                "table": col["table"],
                "field": field,
                "field_desc": col.get("field_desc", ""),
                "aliases": [],
                "unit": "",
                "data_type": data_type,
                "quality_tags": tags,
            }
        )
    return items


def build_join_kb(fks: List[Dict]) -> List[Dict]:
    items = []
    counter: Dict[Tuple[str, str], int] = defaultdict(int)
    for fk in fks:
        left = fk["table"]
        right = fk["ref_table"]
        key = (left, right)
        counter[key] += 1
        suffix = f"_{counter[key]}" if counter[key] > 1 else ""
        join_id = f"{left}_{right}{suffix}"
        items.append(
            {
                "join_path_id": join_id,
                "description": f"{left} to {right}",
                "tables": [left, right],
                "edges": [
                    {
                        "left_table": left,
                        "left_field": fk["column"],
                        "right_table": right,
                        "right_field": fk["ref_column"],
                        "join_type": "inner",
                    }
                ],
            }
        )
    return items


def main() -> None:
    load_dotenv()
    schema = _get_env("TEXT2SQL_MYSQL_DATABASE", "")
    if not schema:
        raise ValueError("TEXT2SQL_MYSQL_DATABASE is required in .env")
    conn = _connect()
    try:
        columns = load_columns(conn, schema)
        fks = load_foreign_keys(conn, schema)
    finally:
        conn.close()

    schema_kb = build_schema_kb(columns, fks)
    join_kb = build_join_kb(fks)

    with open("data/schema_kb.json", "w", encoding="utf-8") as f:
        json.dump(schema_kb, f, ensure_ascii=True, indent=2)
    with open("data/join_kb.json", "w", encoding="utf-8") as f:
        json.dump(join_kb, f, ensure_ascii=True, indent=2)

    print("Updated:")
    print("- data/schema_kb.json")
    print("- data/join_kb.json")
    print("Note: metric_kb.json and template_kb.json are not auto-generated.")


if __name__ == "__main__":
    main()
