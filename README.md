# evaluation
Evaluator for Strata

## Model selection and cost

The eval harness is model-agnostic via OpenAI-compatible API. The default model is `deepseek-chat` (DeepSeek V3.2) via `https://api.deepseek.com`.

**⚠️ EX absolute values are NOT comparable across model tiers — only deltas within the same model and same run conditions are meaningful.**

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `EVAL_MODEL` | `deepseek-chat` | Model name passed to the API |
| `EVAL_BASE_URL` | `https://api.deepseek.com` | API base URL (OpenAI-compatible) |
| `EVAL_API_KEY` | *(none)* | API key; falls back to `DEEPSEEK_API_KEY` |

Override example:
```bash
# Use GPT-4o mini instead (US infra)
EVAL_MODEL=gpt-4o-mini EVAL_BASE_URL=https://api.openai.com/v1 EVAL_API_KEY=<key> python -m src.eval --questions questions/omdb.yaml ...
```

### Benchmark reference

| Model | Tinybird SQL Score | Exactness | Input $/M | ~Cost/100-call run |
|---|---|---|---|---|
| Claude Sonnet 4.6 | 74.25 | 52.41 | $3.00 | ~$1.20 |
| DeepSeek V3.2 | 76.52 | 54.02 | $0.15 | ~$0.06 |
| GPT-4o mini | 72.34 | 44.83 | $0.15 | ~$0.06 |
| Gemini 2.0 Flash-Lite | 70.96 | 42.04 | $0.075 | ~$0.03 |

For a full 50-question × 2-condition run at DeepSeek V3.2 pricing (~$0.15/M input tokens), the estimated cost is **~$0.06 per run** (100 total API calls).

### Privacy note

DeepSeek is a Chinese vendor. **Do not run eval against sensitive or production schemas.** Use public datasets only (OMDB is fine). For US-infra requirements, use `EVAL_MODEL=gpt-4o-mini` with the standard OpenAI base URL.
