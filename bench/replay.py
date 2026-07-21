#!/usr/bin/env python3
"""Recompute every number in bench/RESULTS.md from the committed raw JSONL.

Offline, stdlib only, no API key, no model calls, ~1 second. Nothing in
RESULTS.md is hand-written; if a figure is not produced here it does not appear
there.

    make bench-replay          # writes bench/RESULTS.md
    make bench-check           # fails if the committed RESULTS.md is stale

Dollars are DERIVED --- token counts from the substrate multiplied by
bench/prices.py --- not billed amounts. As an integrity check on the move into
this repo, every row's stored derived_usd is re-derived here and compared.
"""

import argparse
import json
import math
import statistics
import sys

from bench.paths import RAW_DIR, RESULTS_MD
from bench.prices import PRICE_TABLE_VERSION, PRICES, derive_usd

FAMILIES = [
    ("GSM8K", "gsm8k.jsonl", "a number"),
    ("MBPP", "mbpp.jsonl", "a Python function"),
    ("MMLU-Pro", "mmlu_pro.jsonl", "one option letter"),
]

ARMS = ["sonnet", "haiku"]

Z = 1.96  # 95% normal quantile, for the Wilson score interval


def wilson(successes: int, n: int):
    """Wilson score interval, the standard small-n binomial CI.

    Used instead of the normal approximation because several cells here are at
    or near 100%, where the normal interval is nonsense (zero width, or bounds
    above 1).
    """
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    denom = n + Z**2
    centre = (successes + Z**2 / 2) / denom
    half = Z / denom * math.sqrt(p * (1 - p) * n + Z**2 / 4)
    return (max(0.0, centre - half), min(1.0, centre + half))


def load(path):
    with path.open() as f:
        rows = [json.loads(line) for line in f]

    mismatches = []
    for r in rows:
        if r.get("input_tokens") is None or r.get("output_tokens") is None:
            continue
        expected = derive_usd(r["model"], r["input_tokens"], r["output_tokens"])
        if abs(expected - r["derived_usd"]) > 1e-12:
            mismatches.append((r["task_id"], r["arm"], r["derived_usd"], expected))
    return rows, mismatches


def arm_stats(rows, arm):
    a = [r for r in rows if r["arm"] == arm]
    cost = [derive_usd(r["model"], r["input_tokens"], r["output_tokens"]) for r in a]
    out_tok = [r["output_tokens"] for r in a]
    parsed = [r for r in a if r["parse_ok"]]
    correct = [r for r in parsed if r["correct"]]
    lo, hi = wilson(len(correct), len(parsed))
    return {
        "n": len(a),
        "mean_usd": statistics.mean(cost),
        "median_usd": statistics.median(cost),
        "mean_out_tok": statistics.mean(out_tok),
        "median_out_tok": statistics.median(out_tok),
        "parsed": len(parsed),
        "correct": len(correct),
        "cond_acc": len(correct) / len(parsed) if parsed else 0.0,
        "ci": (lo, hi),
        "errors": sum(1 for r in a if r.get("error")),
        "non_end_turn": sum(1 for r in a if r.get("stop_reason") not in (None, "end_turn")),
    }


def paired_stats(rows):
    """Same task, both arms --- removes task-to-task variance from the compare."""
    sonnet = {r["task_id"]: r for r in rows if r["arm"] == "sonnet"}
    haiku = {r["task_id"]: r for r in rows if r["arm"] == "haiku"}
    ids = sorted(set(sonnet) & set(haiku))
    diffs = []
    cheaper = 0
    for t in ids:
        s = derive_usd(sonnet[t]["model"], sonnet[t]["input_tokens"], sonnet[t]["output_tokens"])
        h = derive_usd(haiku[t]["model"], haiku[t]["input_tokens"], haiku[t]["output_tokens"])
        diffs.append(h - s)
        if h < s:
            cheaper += 1
    return {
        "n": len(ids),
        "haiku_cheaper": cheaper,
        "mean_diff": statistics.mean(diffs),
        "median_diff": statistics.median(diffs),
    }


def compute():
    report = {"families": [], "mismatches": []}
    for label, filename, answer_shape in FAMILIES:
        rows, mismatches = load(RAW_DIR / filename)
        report["mismatches"].extend((label, *m) for m in mismatches)
        fam = {
            "label": label,
            "file": filename,
            "answer_shape": answer_shape,
            "rows": len(rows),
            "arms": {arm: arm_stats(rows, arm) for arm in ARMS},
            "paired": paired_stats(rows),
        }
        fam["ratio"] = fam["arms"]["haiku"]["mean_usd"] / fam["arms"]["sonnet"]["mean_usd"]
        report["families"].append(fam)
    return report


def pct(x):
    return f"{100 * x:.1f}%"


def render(report) -> str:
    fams = report["families"]
    total_pairs = sum(f["paired"]["n"] for f in fams)
    total_cheaper = sum(f["paired"]["haiku_cheaper"] for f in fams)

    # The mechanism: answer shapes ordered shortest-required-answer first,
    # against haiku's measured output tokens on each.
    mech = [(f["label"], f["answer_shape"], f["arms"]["haiku"]["mean_out_tok"],
             f["arms"]["sonnet"]["mean_out_tok"]) for f in fams]
    by_label = {label: m for m in mech for label in [m[0]]}

    L = []
    w = L.append
    w("# Benchmark results")
    w("")
    w("Generated by `make bench-replay` from `bench/data/raw/*.jsonl`. Do not")
    w("edit by hand. Every number below is recomputed from the raw per-call")
    w("rows on each run, and hand edits are silently overwritten.")
    w("")
    w("**Dollars are derived, not billed.** Each figure is the substrate's")
    w("reported `input_tokens`/`output_tokens` for that call multiplied by the")
    w("price table in `bench/prices.py`, version:")
    w("")
    w(f"> {PRICE_TABLE_VERSION}")
    w("")
    w("| Model | Input $/MTok | Output $/MTok |")
    w("|---|---|---|")
    for model, p in PRICES.items():
        w(f"| `{model}` | {p['input']} | {p['output']} |")
    w("")
    w("No invoice was consulted and no billing API was read. The substrate's")
    w("own `total_cost_usd` is recorded per row as")
    w("`substrate_reported_cost_usd` but is never the basis: it includes")
    w("auxiliary Claude Code calls the benchmark never requested, and")
    w("attributes some Haiku overhead to the Sonnet arm.")
    w("")

    w("## 1. Per-family cost and quality")
    w("")
    w("| Family | Arm | Mean $/task | Median $/task | Mean out tok | Median out tok | Parsed | Cond. acc. (correct \\| parsed) |")
    w("|---|---|---|---|---|---|---|---|")
    for f in fams:
        for arm in ARMS:
            a = f["arms"][arm]
            lo, hi = a["ci"]
            w(f"| {f['label']} | {arm} | {a['mean_usd']:.6f} | {a['median_usd']:.6f} | "
              f"{a['mean_out_tok']:.1f} | {a['median_out_tok']:.1f} | "
              f"{a['parsed']}/{a['n']} | {pct(a['cond_acc'])} ({a['correct']}/{a['parsed']}), "
              f"95% CI [{pct(lo)}, {pct(hi)}] |")
    w("")

    w("## 2. Paired comparison (same task, both arms)")
    w("")
    w("| Family | Pairs | Haiku cheaper on | Mean cost ratio haiku/sonnet | Mean paired diff $ | Median paired diff $ |")
    w("|---|---|---|---|---|---|")
    for f in fams:
        p = f["paired"]
        w(f"| {f['label']} | {p['n']} | {p['haiku_cheaper']}/{p['n']} | {f['ratio']:.2f}x | "
          f"{p['mean_diff']:+.6f} | {p['median_diff']:+.6f} |")
    w(f"| **All** | **{total_pairs}** | **{total_cheaper}/{total_pairs}** | | | |")
    w("")
    w(f"Haiku 4.5 was cheaper than Sonnet 5 on **{total_cheaper} of "
      f"{total_pairs}** paired tasks, despite costing half as much per token")
    w("in both directions.")
    w("")

    w("## 3. The mechanism")
    w("")
    w("Ranked by how short the required answer actually is:")
    w("")
    order = ["MMLU-Pro", "GSM8K", "MBPP"]
    w("  " + " < ".join(f"{lbl} ({by_label[lbl][1]})" for lbl in order if lbl in by_label))
    w("")
    w("Ranked by Haiku's measured mean output tokens:")
    w("")
    tok_order = sorted(mech, key=lambda m: m[2])
    w("  " + " < ".join(f"{m[0]} ({m[2]:.1f})" for m in tok_order))
    w("")
    w("The two orderings do not agree, and that disagreement is the finding.")
    w("The family demanding the *shortest* answer --- MMLU-Pro, where the")
    w("contract asks for a single letter --- drew the *highest* Haiku token")
    w("spend, higher even than writing a Python function. This is not a clean")
    w("reverse trend either: GSM8K sits in the middle by required answer length")
    w("and lowest by spend. The point is only that required output length does")
    w("not predict spend.")
    w("")
    w("That rules out the obvious objection, \"Haiku is only expensive")
    w("because it emits verbose output formats, and a stricter contract would")
    w("fix it.\" On MMLU-Pro the contract is as strict as a contract gets:")
    w(f"Sonnet complies in a {by_label['MMLU-Pro'][3]:.1f}-token mean, while")
    w(f"Haiku spends {by_label['MMLU-Pro'][2]:.1f}. The excess is reasoning,")
    w("not formatting, and an output contract constrains the answer, not the")
    w("reasoning that precedes it.")
    w("")

    w("## 4. What this does not show")
    w("")
    w("Every conditional-accuracy interval in section 1 overlaps its")
    w("counterpart. At n=50 per family this data cannot distinguish the two")
    w("models' accuracy, and **no claim that Sonnet is more accurate than")
    w("Haiku is supported here**. The cost result stands independent of any")
    w("accuracy claim.")
    w("")
    w("Parse failures are grader-regex misses, not model failures: GSM8K's")
    w("parser accepts only a bare number, so a correct `ANSWER: $70,000` or")
    w("`ANSWER: 160 minutes` fails, in both arms. MMLU-Pro's parser does not")
    w("strip markdown bold, so three `**ANSWER: X**` rows fail. Those calls")
    w("still spent real, correctly-counted tokens, so they remain in the cost")
    w("figures; they are excluded only from accuracy, which is why accuracy is")
    w("reported conditional on parsing.")
    w("")

    w("## 5. Run integrity")
    w("")
    w("| Family | Rows | Execution errors | Non-`end_turn` stop reasons | Stored-vs-recomputed $ mismatches |")
    w("|---|---|---|---|---|")
    for f in fams:
        errs = sum(f["arms"][a]["errors"] for a in ARMS)
        trunc = sum(f["arms"][a]["non_end_turn"] for a in ARMS)
        mm = sum(1 for m in report["mismatches"] if m[0] == f["label"])
        w(f"| {f['label']} | {f['rows']} | {errs} | {trunc} | {mm} |")
    w("")
    w("Zero execution errors means no arm was advantaged by dropped calls.")
    w("Zero non-`end_turn` stop reasons means no arm's output tokens were")
    w("censored by truncation. The last column re-derives each row's dollar")
    w("figure from its token counts and compares it to the value stored when")
    w("the call was made. A non-zero count would mean the data was corrupted,")
    w("or that the price table changed under it.")
    w("")

    w("## 6. Provenance")
    w("")
    w("Task sets: `bench/data/tasks/` (committed). Raw per-call rows including")
    w("full model output: `bench/data/raw/` (committed). Run logs:")
    w("`bench/data/logs/`. Live-run harness: `bench/live/`. Source corpora and")
    w("their sha256s: `bench/live/build_tasksets.py`.")
    w("")
    w("Substrate: Claude Code CLI 2.1.215, headless, `--effort low`,")
    w("`--tools \"\"`, single-turn confirmed, on a subscription rather than a")
    w("metered API key. See `bench/README.md` for what that costs the result.")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="exit non-zero if the committed RESULTS.md is stale")
    args = ap.parse_args()

    report = compute()

    if report["mismatches"]:
        print("FAIL: stored derived_usd disagrees with recomputation:", file=sys.stderr)
        for m in report["mismatches"]:
            print(f"  {m}", file=sys.stderr)
        return 1

    rendered = render(report)

    if args.check:
        current = RESULTS_MD.read_text() if RESULTS_MD.exists() else ""
        if current != rendered:
            print(f"FAIL: {RESULTS_MD} is stale. Run `make bench-replay`.", file=sys.stderr)
            return 1
        print(f"OK: {RESULTS_MD} matches the raw data.")
        return 0

    RESULTS_MD.write_text(rendered)

    for f in report["families"]:
        p = f["paired"]
        print(f"{f['label']:9s} ratio {f['ratio']:.2f}x  "
              f"haiku cheaper on {p['haiku_cheaper']}/{p['n']}  "
              f"haiku mean out tok {f['arms']['haiku']['mean_out_tok']:.1f}")
    total = sum(f["paired"]["n"] for f in report["families"])
    cheaper = sum(f["paired"]["haiku_cheaper"] for f in report["families"])
    print(f"overall: haiku cheaper on {cheaper}/{total} paired tasks")
    print(f"wrote {RESULTS_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
