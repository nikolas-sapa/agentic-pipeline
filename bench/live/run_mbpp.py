#!/usr/bin/env python3
"""Family 2/3: MBPP (sanitized) Python function synthesis.

Live run --- spends subscription quota. Overwrites bench/data/raw/mbpp.jsonl.
Replaying the committed data instead: `make bench-replay`.

    uv run python -m bench.live.run_mbpp
"""

import re
import subprocess

from bench.live.runner import ARMS, base_row, error_row, invoke, prompt_hash, sweep, usage_fields
from bench.paths import RAW_DIR, TASKS_DIR

SYSTEM_PROMPT = "You are a careful Python programmer."

CONTRACT = ("Write a single Python function that solves the task. Respond with "
            "ONLY a fenced ```python code block containing the function "
            "definition, and nothing else before or after it.")

TASKS_FILE = TASKS_DIR / "mbpp_50.jsonl"
OUT_FILE = RAW_DIR / "mbpp.jsonl"

CODEBLOCK_RE = re.compile(r"```(?:python)?\s*\n(.*?)```", re.DOTALL)


def build_user_prompt(task_prompt: str, test_list) -> str:
    tests_preview = "\n".join(test_list[:1])  # one example test, signature hint only
    return (f"{task_prompt}\n\nExample of how it will be called:\n{tests_preview}"
            f"\n\n{CONTRACT}")


def extract_code(text: str):
    m = CODEBLOCK_RE.search(text)
    if not m:
        return None, False
    return m.group(1).strip(), True


def grade(code: str, test_imports, test_list):
    """Run model-authored code against MBPP asserts in a separate subprocess.

    Model-authored code is executed, so: separate interpreter (never in-process
    exec), -I to ignore the ambient environment, sockets stubbed out, minimal
    PATH, hard 10s timeout. These are not a security sandbox: code still has
    host filesystem/process access and can bypass the socket patch. Run live
    grading only inside a disposable sandbox. Returns (ran, correct).
    """
    if code is None:
        return False, False
    prelude = "\n".join(test_imports) if test_imports else ""
    script = (
        "import socket\n"
        "def _no_net(*a, **k):\n"
        "    raise RuntimeError('network disabled in grader')\n"
        "socket.socket = _no_net\n"
        "socket.create_connection = _no_net\n"
        f"{prelude}\n"
        f"{code}\n"
        + "\n".join(test_list) + "\n"
    )
    try:
        proc = subprocess.run(
            ["python3", "-I", "-c", script],
            capture_output=True, text=True, timeout=10,
            env={"PATH": "/usr/bin:/bin"},
        )
        return True, proc.returncode == 0
    except subprocess.TimeoutExpired:
        return True, False
    except Exception:
        return False, False


def run_one(task, arm_name):
    model = ARMS[arm_name]
    user_prompt = build_user_prompt(task["prompt"], task["test_list"])
    ph = prompt_hash(SYSTEM_PROMPT, user_prompt)

    result, err, latency_ms = invoke(model, SYSTEM_PROMPT, user_prompt)

    row = base_row(task["task_id"], arm_name, model, ph, result, latency_ms)
    if result is None:
        row.update(error_row(err))
        return row

    text = result.get("result", "") or ""
    code, parse_ok = extract_code(text)
    _ran, correct = grade(code, task.get("test_imports", []), task["test_list"])

    row.update(usage_fields(result, model))
    row.update({
        "parse_ok": parse_ok,
        "correct": bool(parse_ok and correct),
        "raw_result_text": text,
    })
    return row


if __name__ == "__main__":
    sweep(TASKS_FILE, OUT_FILE, run_one)
