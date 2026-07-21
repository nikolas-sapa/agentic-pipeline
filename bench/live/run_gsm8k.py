#!/usr/bin/env python3
"""Family 1/3: GSM8K arithmetic word problems.

Live run --- spends subscription quota. Overwrites bench/data/raw/gsm8k.jsonl.
Replaying the committed data instead: `make bench-replay`.

    uv run python -m bench.live.run_gsm8k
"""

import re

from bench.live.runner import ARMS, base_row, error_row, invoke, prompt_hash, sweep, usage_fields
from bench.paths import RAW_DIR, TASKS_DIR

SYSTEM_PROMPT = "You are a careful math word-problem solver."

CONTRACT = ("Show at most three lines of working. End with `ANSWER: <value>` "
            "and nothing after it.")

TASKS_FILE = TASKS_DIR / "gsm8k_50.jsonl"
OUT_FILE = RAW_DIR / "gsm8k.jsonl"


def build_user_prompt(question: str) -> str:
    return f"{question}\n\n{CONTRACT}"


def parse_answer(text: str):
    """Return (value_or_None, parse_ok).

    Known brittleness, left as-run: this accepts only a bare number, so a
    correct answer written `ANSWER: $70,000` or `ANSWER: 160 minutes` fails to
    parse. That is why RESULTS.md reports conditional accuracy (correct given
    parsed) and not a raw pass rate --- the failures are grader-regex misses,
    they hit both arms, and they do not touch the token counts the cost result
    is built from. Fixing the regex here would change what the committed data
    means, so it stays as it ran.
    """
    m = re.search(r"ANSWER:\s*(-?[\d,]*\.?\d+)\s*$", text.strip())
    if not m:
        return None, False
    raw = m.group(1).replace(",", "")
    try:
        val = float(raw)
        if val == int(val):
            val = int(val)
        return val, True
    except ValueError:
        return None, False


def reference_answer(answer_field: str):
    raw = answer_field.split("####")[-1].strip().replace(",", "")
    val = float(raw)
    if val == int(val):
        val = int(val)
    return val


def run_one(task, arm_name):
    model = ARMS[arm_name]
    user_prompt = build_user_prompt(task["question"])
    ph = prompt_hash(SYSTEM_PROMPT, user_prompt)

    result, err, latency_ms = invoke(model, SYSTEM_PROMPT, user_prompt)

    row = base_row(task["task_id"], arm_name, model, ph, result, latency_ms)
    if result is None:
        row.update(error_row(err))
        return row

    text = result.get("result", "") or ""
    parsed_val, parse_ok = parse_answer(text)
    ref_val = reference_answer(task["answer"])

    row.update(usage_fields(result, model))
    row.update({
        "parse_ok": parse_ok,
        "parsed_value": parsed_val,
        "reference_value": ref_val,
        "correct": parse_ok and parsed_val == ref_val,
        "raw_result_text": text,
    })
    return row


if __name__ == "__main__":
    sweep(TASKS_FILE, OUT_FILE, run_one)
