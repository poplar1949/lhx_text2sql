import json
from typing import List

from jsonschema import Draft7Validator

from app.core.models import ValidationError


def load_schema(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_plan(plan: dict, schema: dict) -> List[ValidationError]:
    validator = Draft7Validator(schema)
    errors: List[ValidationError] = []
    for err in validator.iter_errors(plan):
        field_path = ".".join([str(p) for p in err.path]) or "$"
        errors.append(
            ValidationError(
                code="schema",
                message=err.message,
                field_path=field_path,
                suggestions=[],
            )
        )
    return errors
