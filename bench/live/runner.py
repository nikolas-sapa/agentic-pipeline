"""Shared live-run machinery for the three family runners.

This is the path that spends real quota. It invokes the Claude Code CLI in
headless mode against an interactive subscription --- see bench/README.md for
why that substrate was chosen and what it costs the result in interpretability.
There is no metered-API path here; porting to one is possible but would need
its own re-validation, because CLI and API token accounting are not the same
measurement.

The code below was lifted unchanged from the scripts that produced
bench/data/raw/*.jsonl. Only the byte-identical parts of the three runners are
shared here; prompt construction, output contract, and grading stay per-family
because they legitimately differ. Nothing that touches a number was altered in
the move.
"""

import hashlib
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bench.prices import derive_usd

CLAUDE_BIN = "/Users/nikolassapalidis/.local/bin/claude"

ARMS = {
    "sonnet": "claude-sonnet-5",
    "haiku": "claude-haiku-4-5-20251001",
}

# Flags fixed by 03-router-benchmark.md §0.2. --effort low pins thinking so the
# arms are comparable; --tools "" + --setting-sources "" + --system-prompt
# suppress the CLI's own 21k-token scaffolding, which otherwise dominates the
# measurement 40x over.
CLI_FLAGS = [
    "--output-format", "json",
    "--effort", "low",
    "--tools", "",
    "--setting-sources", "",
    "--disable-slash-commands",
    "--strict-mcp-config",
    "--no-session-persistence",
]


def prompt_hash(system_prompt: str, user_prompt: str) -> str:
    """Hash of the exact bytes sent, independent of which arm receives them.

    Recorded per row so a reader can verify the two arms got identical input.
    Note honestly what this does and does not prove: it cannot differ by arm by
    construction, so it is a restatement of the real guarantee (run_one never
    branches on arm; only --model differs) rather than an independent check.
    """
    return hashlib.sha256((system_prompt + "\x00" + user_prompt).encode()).hexdigest()


def invoke(model: str, system_prompt: str, user_prompt: str):
    """One CLI call. Returns (parsed_json_or_None, error_or_None, latency_ms)."""
    cmd = [
        CLAUDE_BIN, "-p", user_prompt,
        "--model", model,
        "--system-prompt", system_prompt,
        *CLI_FLAGS,
    ]

    t0 = time.time()
    max_retries = 4
    backoff = 5
    result = None
    err = None
    latency_ms = None
    for _attempt in range(max_retries):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            latency_ms = int((time.time() - t0) * 1000)
            if proc.returncode != 0:
                err = f"returncode={proc.returncode} stderr={proc.stderr[:500]}"
                out = proc.stdout.strip()
                if "rate" in (proc.stderr + out).lower() or "429" in (proc.stderr + out):
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                break
            result = json.loads(proc.stdout)
            break
        except subprocess.TimeoutExpired:
            err = "timeout"
            time.sleep(backoff)
            backoff *= 2
        except json.JSONDecodeError as e:
            err = f"json_decode_error: {e}; stdout={proc.stdout[:300]}"
            break

    return result, err, latency_ms


def base_row(task_id, arm_name, model, ph, result, latency_ms):
    return {
        "task_id": task_id,
        "arm": arm_name,
        "model": model,
        "prompt_hash": ph,
        "latency_ms": latency_ms if result else None,
    }


def error_row(err):
    return {
        "error": err,
        "correct": False,
        "parse_ok": False,
        "derived_usd": None,
        "substrate_reported_cost_usd": None,
        "input_tokens": None,
        "output_tokens": None,
        "stop_reason": None,
    }


def usage_fields(result, model):
    """Token counts and the derived dollar figure for one completed call.

    The cost basis is usage.input_tokens/output_tokens, never the substrate's
    total_cost_usd --- that figure includes auxiliary Claude Code calls the
    benchmark never requested (03-router-benchmark.md §0.2). It is recorded
    alongside so a reader can see the gap.
    """
    usage = result.get("usage", {})
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    usd = None
    if input_tokens is not None and output_tokens is not None:
        usd = derive_usd(model, input_tokens, output_tokens)
    return {
        "substrate_reported_cost_usd": result.get("total_cost_usd"),
        "derived_usd": usd,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "stop_reason": result.get("stop_reason"),
    }


def sweep(tasks_file, out_file, run_one):
    """Run every task against both arms, concurrency 2, and write the JSONL."""
    with open(tasks_file) as f:
        tasks = [json.loads(line) for line in f]

    jobs = [(t, arm) for t in tasks for arm in ARMS]

    results = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(run_one, t, arm): (t["task_id"], arm) for t, arm in jobs}
        done = 0
        for fut in as_completed(futs):
            row = fut.result()
            results.append(row)
            done += 1
            print(f"[{done}/{len(jobs)}] task={row['task_id']} arm={row['arm']} "
                  f"correct={row.get('correct')} parse_ok={row.get('parse_ok')} "
                  f"derived_usd={row.get('derived_usd')} err={row.get('error')}",
                  file=sys.stderr)

    results.sort(key=lambda r: (r["task_id"], r["arm"]))
    with open(out_file, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    print(f"wrote {len(results)} rows to {out_file}", file=sys.stderr)
