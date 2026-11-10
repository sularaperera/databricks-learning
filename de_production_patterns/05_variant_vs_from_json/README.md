# 05 · Semi-Structured Data at Scale: VARIANT vs from_json

**Scenario:** a payment gateway sends webhooks of many types (`payment.*`, `refund.created`, `dispute.opened`,
`customer.updated`, `payout.paid`). Mid-way, an API version change turns `amount` from a number into an object, an
old SDK sends amounts as strings, `metadata` keys differ per merchant, and one payload is truncated.

## Contents

```
05_variant_vs_from_json/
├── notebooks/
│   └── 01_variant_vs_from_json.py
└── sample_data/
    └── payment_webhooks.jsonl     # 14 events, 6 types, 2 API versions, 1 broken line
```

## What you will learn

| Topic | Where |
|---|---|
| Bronze with a `raw` string + `try_parse_json` VARIANT, using `read_files` | §1 |
| `from_json` + `schema_of_json`, and the **silent NULLs / dropped fields** it produces | §2 |
| VARIANT paths, `::` casts, `variant_get` vs `try_variant_get` | §3 |
| Discovering shapes with `schema_of_variant_agg` (spot the API change) | §3 |
| Handling two API versions in one expression | §3 |
| `variant_explode` for arrays and free-form maps; JSON null vs missing; VARIANT → STRUCT cast | §3 |
| The `singleVariantColumn` reader option | §3 |
| Performance on 1M events: from_json vs get_json_object vs VARIANT vs typed columns | §4 |
| The hybrid silver table (typed hot columns + VARIANT payload) and per-domain views | §5 |
| VARIANT limitations | §5 |

## Run

Run all cells. Defaults: catalog `workspace`, schema `variant_demo`, volume `raw`.
