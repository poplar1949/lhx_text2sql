from app.core.schema import load_schema, validate_plan


def test_schema_validation_missing_fields():
    schema = load_schema("schemas/plan_dsl.schema.json")
    invalid_plan = {"version": "1.0"}
    errors = validate_plan(invalid_plan, schema)
    assert errors
