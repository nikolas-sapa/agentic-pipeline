#!/usr/bin/env python3
"""Family 3/3: MMLU-Pro multiple-choice knowledge.

Live run --- spends subscription quota. Overwrites bench/data/raw/mmlu_pro.jsonl.
Replaying the committed data instead: `make bench-replay`.

    uv run python -m bench.live.run_mmlu_pro
"""

import re

from bench.live.runner import ARMS, base_row, error_row, invoke, prompt_hash, sweep, usage_fields
from bench.paths import RAW_DIR, TASKS_DIR

SYSTEM_PROMPT = "You are a careful, knowledgeable exam-taker."

CONTRACT = "Respond with `ANSWER: <letter>` and nothing else."

TASKS_FILE = TASKS_DIR / "mmlu_pro_50.jsonl"
OUT_FILE = RAW_DIR / "mmlu_pro.jsonl"


def build_user_prompt(question: str, options, letters) -> str:
    opts = "\n".join(f"{l}. {o}" for l, o in zip(letters, options))
    return f"{question}\n\n{opts}\n\n{CONTRACT}"


def parse_answer(text: str, letters):
    """Known brittleness, left as-run: markdown bold is not stripped, so
    `**ANSWER: C**` fails to parse. Three haiku rows are lost to this. As with
    GSM8K's regex, it is a grader gap that does not touch the token counts, and
    changing it would change what the committed data means.
    """
    m = re.search(r"ANSWER:\s*([A-J])\s*$", text.strip())
    if not m:
        return None, False
    letter = m.group(1)
    if letter not in letters:
        return None, False
    return letter, True


def run_one(task, arm_name):
    model = ARMS[arm_name]
    letters = task["letters"]
    user_prompt = build_user_prompt(task["question"], task["options"], letters)
    ph = prompt_hash(SYSTEM_PROMPT, user_prompt)

    result, err, latency_ms = invoke(model, SYSTEM_PROMPT, user_prompt)

    row = base_row(task["task_id"], arm_name, model, ph, result, latency_ms)
    if result is None:
        row.update(error_row(err))
        return row

    text = result.get("result", "") or ""
    parsed_letter, parse_ok = parse_answer(text, letters)

    row.update(usage_fields(result, model))
    row.update({
        "parse_ok": parse_ok,
        "parsed_letter": parsed_letter,
        "reference_letter": task["answer_letter"],
        "correct": bool(parse_ok and parsed_letter == task["answer_letter"]),
        "raw_result_text": text,
    })
    return row


if __name__ == "__main__":
    sweep(TASKS_FILE, OUT_FILE, run_one)
