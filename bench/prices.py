"""The price table the benchmark's dollar figures are derived from.

Dollars in RESULTS.md are DERIVED: token counts reported by the substrate,
multiplied by this table. They are not billed amounts and no invoice was
consulted. The substrate's own `total_cost_usd` is recorded in the raw JSONL
as `substrate_reported_cost_usd` but is deliberately never the basis --- it
includes harness-side calls the benchmark never requested, and attributes
Haiku overhead to the Sonnet arm.

Source: https://platform.claude.com/docs/en/about-claude/pricing, fetched
2026-07-20. Sonnet 5's $2/$10 is introductory pricing that ends 2026-08-31;
from 2026-09-01 it becomes $3/$15, which would widen every ratio below in
Haiku's favour without closing any of them.
"""

PRICE_TABLE_VERSION = "2026-07-20 (Sonnet 5 intro rate, valid through 2026-08-31)"

# $ per 1M tokens.
PRICES = {
    "claude-sonnet-5": {"input": 2, "output": 10},
    "claude-haiku-4-5-20251001": {"input": 1, "output": 5},
}


def derive_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    prices = PRICES[model]
    return (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000
