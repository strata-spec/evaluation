from src.scorer import _execute_sql_detailed, load_questions

DB = "postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require"


def main() -> int:
    questions = load_questions("questions/omdb.yaml")
    exec_errors: list[str] = []
    zero_rowsets: list[str] = []

    for question in questions:
        result, error = _execute_sql_detailed(DB, question["reference_sql"])
        if result is None:
            exec_errors.append(f"{question['id']}: {error}")
            continue
        _, rows = result
        if len(rows) == 0:
            zero_rowsets.append(question["id"])

    print(f"TOTAL={len(questions)}")
    print(f"EXEC_ERRORS={exec_errors}")
    print(f"ZERO_ROWSETS={zero_rowsets}")
    print(f"PASS={not exec_errors and not zero_rowsets}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
