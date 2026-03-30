# Strata Eval - Reorganized Project

## Overview

This workspace has been reorganized for better maintainability and to support Strata integration. All components are functional and verified to work together.

## Folder Structure

```
eval-strata/
├── src/                    ← Python source code
│   ├── eval.py            ← Main evaluation script with CLI
│   ├── scorer.py          ← SQL execution and result comparison
│   ├── reporter.py        ← Report generation from results
│   ├── smif_loader.py     ← Semantic model loading and merging
│   └── verify_questions.py ← Question verification utility
│
├── tests/                 ← Unit test suite (47 tests, all passing)
│   ├── test_eval.py
│   ├── test_scorer.py
│   ├── test_reporter.py
│   └── test_smif_loader.py
│
├── config/               ← Configuration files
│   ├── semantic.yaml     ← Strata-generated semantic model
│   ├── semantic.json     ← Alternative format
│   └── corrections.yaml  ← Human corrections (append-only)
│
├── questions/            ← Test questions
│   └── omdb.yaml         ← 25 OMDB test questions (5 per tier)
│
├── explore/              ← Exploratory scripts (for development)
│   ├── omdb_explore.py
│   ├── omdb_explore2.py
│   ├── omdb_explore3.py
│   ├── omdb_explore4.py
│   └── omdb_explore5.py
│
├── strata/               ← Strata integration
│   ├── config/           ← Strata configuration files (to be added)
│   └── output/           ← Strata-generated outputs
│
├── results/              ← Evaluation results (gitignored)
│   ├── results_*.json    ← Per-question raw data
│   └── report_*.md       ← Triage reports
│
├── pyproject.toml        ← Project metadata and dependencies
├── eval.md              ← Detailed project documentation
├── verify_setup.py      ← Non-LLM verification script (run this first!)
└── .venv/               ← Python virtual environment (gitignored)
```

## Getting Started

### 1. Verify the Setup (Non-LLM)

Run the verification script to confirm everything is properly organized:

```bash
python verify_setup.py
```

Expected output:
- ✓ All 9 directory checks pass
- ✓ 25 questions loaded
- ✓ Semantic model loaded (24 models, 27 relationships)
- ✓ Corrections loaded and merged
- ✓ Reporter generates reports successfully

### 2. Run Unit Tests

All 47 unit tests verify the core functionality:

```bash
python -m unittest discover tests/ -v
```

Or with Python environment:
```bash
.venv/bin/python -m unittest discover tests/ -v
```

### 3. Run Evaluation (with LLM and Database)

For the full evaluation with LLM calls:

```bash
python -m src.eval \
  --db postgres://user:pass@host:port/omdb \
  --semantic config/semantic.yaml \
  --corrections config/corrections.yaml \
  --questions questions/omdb.yaml \
  --model claude-sonnet-4-20250514 \
  --output results/
```

Optional flags:
- `--tier simple` - Run only questions from one tier
- `--baseline-only` - Skip Strata condition
- `--strata-only` - Skip baseline condition

## File Organization Benefits

| Item | Location | Purpose |
|------|----------|---------|
| Source code | `src/` | Importable as package, organized by functionality |
| Tests | `tests/` | Organized test suite, run with unittest discovery |
| Configuration | `config/` | Semantic models and corrections separate from code |
| Questions | `questions/` | Test data clearly separated from code |
| Exploration | `explore/` | Development scripts kept organized |
| Strata | `strata/` | Dedicated directory for Strata integration |
| Results | `results/` | Output directory clearly marked (gitignored) |

## Path Updates Made

### Updated Default CLI Paths
- `--semantic`: `./semantic.yaml` → `../config/semantic.yaml` (when run from `src/`)
- `--corrections`: `./corrections.yaml` → `../config/corrections.yaml` (when run from `src/`)  
- `--questions`: `./questions/omdb.yaml` → `../questions/omdb.yaml` (when run from `src/`)
- `--output`: `./results/` → `../results/` (when run from `src/`)

### Updated Imports
All source files use sys.path manipulation to work both as:
1. Direct script execution: `python src/eval.py`
2. Module import: `python -m src.eval`

Test files include path injection: `sys.path.insert(0, str(Path(__file__).parent.parent / "src"))`

## Next Steps: Adding Strata

1. **Place Strata configuration** in `strata/config/`
2. **Generate semantic model** - Save to `config/semantic.yaml`
3. **Add corrections** if needed - Create or update `config/corrections.yaml`
4. **Run full evaluation** with your database connection

## Dependencies

All packages installed in `.venv`:
- `anthropic` - Claude API client
- `psycopg2-binary` - PostgreSQL driver
- `pyyaml` - YAML parsing
- `sqlglot` - SQL parsing and analysis

Install with: `pip install -r requirements.txt` (or configure Python environment)

## Testing & Verification Status

✓ **Verified Working:**
- File structure reorganization complete
- All 47 unit tests passing
- Non-LLM verification suite passing
- Import paths working for both execution methods
- Report generation working with mock data
- Semantic model loading and merging working

✓ **Ready for:**
- Adding Strata configuration
- Running evaluation with Postgres database
- Generating evaluation reports
- Testing with different question sets

## Documentation

- **eval.md** - Detailed project documentation with expanded file structure section
- **pyproject.toml** - Project metadata and dependencies
- **This file** - Quick-start guide and reorganization summary

---

**Reorganization Date:** March 24, 2026  
**Verification Script:** `verify_setup.py` ✓ All tests passed
