# Strata Eval

Internal dev tool. Measures whether Strata's SMIF semantic layer improves LLM SQL accuracy on OMDB Postgres. Not shipped, not released.

---
ALWAYS USE UV

## Project context

**Strata** (`github.com/strata-spec/openstrata`) — Go CLI that infers a SMIF semantic model from Postgres and serves it via MCP.  
**SMIF** (`github.com/strata-spec/spec`) — the open spec. Strata is the reference implementation.

### Three-file system
| File | Owner | Notes |
|---|---|---|
| `strata.md` | Human (before init) | Domain context injected into LLM calls |
| `semantic.yaml` | Tool-generated | Never manually edited. Overwritten on re-inference. |
| `corrections.yaml` | Human (append-only) | Always takes precedence. Never overwritten by tools. |

**The Strata eval condition uses the merged model** — `semantic.yaml` + `corrections.yaml` via `ApplyOverlay`, not raw `semantic.yaml`. That's the only state that exists in production.

### SMIF model contents (what gets injected)
- `domain` — database-level name + description
- `models` — per table: `model_id`, `label`, `description`, `grain`, `physical_source`, `primary_key`
- `columns` — per column: `name`, `data_type`, `role` (identifier/dimension/measure/timestamp/flag), `label`, `description`, `example_values`, `difficulty` (self_evident/context_dependent/ambiguous/domain_dependent), `provenance`, `confidence`
- `relationships` — join paths: `from_model`, `from_column`, `to_model`, `to_column`, `join_condition`
- `metrics` / `concepts` — if present

---

## Two conditions

**Baseline:** raw `CREATE TABLE` + FK constraints. No semantic context.  
**Strata:** merged SMIF model injected as structured context. Only models relevant to the question (keyword match on name/description), not the full file.

Same LLM, same DB, same question. One variable.

---

## Question set (`questions/omdb.yaml`)

```yaml
- id: "q001"
  tier: simple
  natural_language: "How many movies are in the database?"
  reference_sql: "SELECT COUNT(*) FROM movies"
  notes: "Unambiguous single table."
```

### Tiers
| Tier | Tests | Expected signal |
|---|---|---|
| `simple` | Single table, unambiguous columns | Small delta — both should score well |
| `ambiguous` | Generic names: `type`, `value`, `rating`, `status` | Largest Strata gain |
| `multi_table` | Correct join path selection | Significant gain on join accuracy |
| `metric` | Business concept resolution — which `rating` column? | Significant gain |
| `gotcha` | Tables/columns that look right but are semantically wrong | Strongest signal |

Start with 5 questions per tier (25 total). Expand to 10 per tier once harness is validated.

**Reference SQL rules:** must execute and return ≥1 row. Must be the verified canonical answer. Ambiguous questions (multiple valid answers) are excluded.

---

## Metrics

**EX — Execution Accuracy** (primary): SQL executes and returns a result set equivalent to reference. Equivalence: same rows after sort, float normalisation to 2dp, NULL normalisation, lowercase strings.

**QE — Query Error Rate**: fraction of queries that fail to execute. Measures schema hallucination.

**SL — Schema Linking**: did generated SQL reference the correct tables/columns? Parsed via `sqlglot`, compared to reference SQL's tables/columns. Diagnostic: SL correct + EX wrong = logic error. SL wrong = schema confusion.

Not tracked: EM (useless), latency (separate experiment), correction rate (= 1−EX).

All metrics reported per tier + aggregate + delta (Strata − Baseline).

---

## Output

**`results.json`** — per-question raw data (id, tier, condition, generated SQL, execution_success, result_match, schema_link_correct, failure_mode).

**`report.md`** — developer triage document. This is the primary output.

Report structure:
1. Summary table (EX/QE/SL baseline vs strata vs delta, by tier)
2. One block per failing question: question, both SQLs, result diff, failure mode classification
3. Both-fail summary table — most diagnostic, candidates for issues
4. Patterns / observations

---

## Failure mode taxonomy

| Class | Meaning | Fix target |
|---|---|---|
| `inference · coarse pass` | Wrong/missing table description | Go code — coarse LLM prompt |
| `inference · fine pass` | Wrong/missing column description, role, or difficulty | Go code — fine LLM prompt |
| `inference · join inference` | Wrong or missing join condition | Go code — join heuristics |
| `inference · schema hallucination` | Agent invented non-existent table/column | Go code — model completeness |
| `spec gap` | SMIF has no field to express what agent needed | Spec change + Go code |
| `question` | Bad reference SQL, ambiguous question, out of v0 scope | Fix question set |

Developer reads `report.md`, triages each failure to a GitHub issue on `openstrata`, `strata-spec/spec`, or the question set. Re-runs eval after fixes to measure delta improvement. No automation. Developer is the loop.

---

## File structure

```
eval-strata/
├── src/
│   ├── eval.py                      ← main script + CLI flags
│   ├── smif_loader.py               ← loads + merges semantic.yaml + corrections.yaml (ApplyOverlay)
│   ├── scorer.py                    ← EX, QE, SL metrics
│   ├── reporter.py                  ← generates report.md from results.json
│   └── verify_questions.py           ← utility to verify reference SQL execution
├── tests/
│   ├── test_eval.py
│   ├── test_scorer.py
│   ├── test_reporter.py
│   └── test_smif_loader.py
├── config/
│   ├── semantic.yaml               ← Strata-generated semantic model
│   ├── semantic.json               ← Alternative format
│   └── corrections.yaml            ← Human corrections (append-only, takes precedence)
├── questions/
│   └── omdb.yaml                   ← Test question set
├── explore/
│   ├── omdb_explore.py
│   ├── omdb_explore2.py
│   ├── omdb_explore3.py
│   ├── omdb_explore4.py
│   └── omdb_explore5.py             ← Exploratory query scripts
├── strata/
│   ├── config/                      ← Strata configuration files (to be added)
│   └── output/                      ← Strata-generated outputs
├── results/                         ← Evaluation output (gitignored)
│   ├── results_YYYY-MM-DDTHH-MM-SS.json
│   └── report_YYYY-MM-DDTHH-MM-SS.md
├── pyproject.toml
├── eval.md                          ← this file
└── .venv/                           ← Python virtual environment (gitignored)
```

## CLI

```bash
# From project root
python -m src.eval \
  --db postgres://user:pass@localhost:5432/omdb \
  --semantic config/semantic.yaml \
  --corrections config/corrections.yaml \
  --questions questions/omdb.yaml \
  --output results/

# Or from src/ directory  
python eval.py \
  --db postgres://user:pass@localhost:5432/omdb \
  --semantic ../config/semantic.yaml \
  --corrections ../config/corrections.yaml \
  --questions ../questions/omdb.yaml \
  --output ../results/

# Optional flags
--model deepseek-chat    # override model (default: EVAL_MODEL env or deepseek-chat)
--tier simple          # single tier only
--baseline-only        # skip strata condition
--strata-only          # skip baseline condition
```

## Dependencies
`openai` · `psycopg2` · `pyyaml` · `sqlglot`

## Invariants
1. Never modify `semantic.yaml` or `corrections.yaml` — eval is read-only
2. Strata condition = merged model, not raw `semantic.yaml`
3. Result comparison by normalised result set, not SQL string
4. Reference SQL returning zero rows = invalid question, excluded from scoring
5. ALWAYS USE UV