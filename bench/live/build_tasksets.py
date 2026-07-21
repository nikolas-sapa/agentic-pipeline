#!/usr/bin/env python3
"""Rebuild the sampled n=50 task sets in bench/data/tasks/ from source corpora.

The sampled task sets are committed, so this only needs running if you want to
verify the sampling or draw a different sample. The source corpora are NOT
committed (4MB parquet, 750KB JSONL); fetch them into a directory of your
choice and point --corpora at it. Expected sha256s are asserted below, so a
different upstream revision fails loudly instead of silently reshuffling the
benchmark.

    GSM8K     test.jsonl          https://raw.githubusercontent.com/openai/grade-school-math/master/grade_school_math/data/test.jsonl
    MBPP      mbpp_sanitized.json google-research/google-research mirror of the sanitized split
    MMLU-Pro  mmlu_pro_test.parquet  TIGER-Lab/MMLU-Pro, test split

    uv run python -m bench.live.build_tasksets --corpora ~/corpora

MMLU-Pro needs pandas + pyarrow to read the parquet; neither is a dependency of
the pipeline itself, so install them ad hoc if you rebuild that family.
"""

import argparse
import hashlib
import json
import random
from pathlib import Path

from bench.paths import TASKS_DIR

SEED = 20260720  # the run date, recorded so the sample is reproducible

EXPECTED_SHA256 = {
    "test.jsonl": "3730d312f6e3440559ace48831e51066acaca737f6eabec99bccb9e4b3c39d14",
    "mbpp_sanitized.json": "ca95deaa9a01ef0a6f439f88bcf0dd3db3563d22f22aad6cae04ebb9a8d8c8e9",
    "mmlu_pro_test.parquet": "0e24a191921c2f453518a537a8b2117bd137e7714d4ef1565e9ba06c1ecb9ad8",
}


def checked(corpora: Path, name: str) -> Path:
    path = corpora / name
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != EXPECTED_SHA256[name]:
        raise SystemExit(
            f"{name}: sha256 {digest} != expected {EXPECTED_SHA256[name]}. "
            "The upstream corpus differs from the one the committed results "
            "were sampled from; resolve that before rebuilding."
        )
    print(f"{name}: sha256 ok")
    return path


def build_gsm8k(corpora: Path):
    """First 50 lines of the test split, task_id 0-49. No sampling."""
    src = checked(corpora, "test.jsonl")
    with src.open() as f:
        rows = [json.loads(line) for line in f][:50]
    with (TASKS_DIR / "gsm8k_50.jsonl").open("w") as f:
        for i, r in enumerate(rows):
            f.write(json.dumps({"question": r["question"], "answer": r["answer"],
                                "task_id": i}) + "\n")
    print(f"GSM8K: wrote {len(rows)} tasks (first 50, no sampling)")


def build_mbpp(corpora: Path):
    src = checked(corpora, "mbpp_sanitized.json")
    mbpp = json.loads(src.read_text())

    mbpp_sorted = sorted(mbpp, key=lambda t: t["task_id"])
    rng = random.Random(SEED)
    sample = rng.sample(mbpp_sorted, 50)
    sample.sort(key=lambda t: t["task_id"])

    with (TASKS_DIR / "mbpp_50.jsonl").open("w") as f:
        for t in sample:
            f.write(json.dumps({
                "task_id": t["task_id"],
                "prompt": t["prompt"],
                "test_list": t["test_list"],
                "test_imports": t.get("test_imports", []),
            }) + "\n")
    print(f"MBPP: sampled {len(sample)} of {len(mbpp)}, seed={SEED}")


def build_mmlu_pro(corpora: Path):
    """Stratified: round-robin over subjects, subject order and within-subject
    order both shuffled from the same seed."""
    import pandas as pd

    src = checked(corpora, "mmlu_pro_test.parquet")
    df = pd.read_parquet(src)

    rng = random.Random(SEED)
    subjects = sorted(df["category"].unique().tolist())
    rng.shuffle(subjects)
    per_subject_pool = {s: df[df["category"] == s].index.tolist() for s in subjects}
    for s in per_subject_pool:
        rng.shuffle(per_subject_pool[s])

    picked_idx = []
    i = 0
    while len(picked_idx) < 50:
        pool = per_subject_pool[subjects[i % len(subjects)]]
        if pool:
            picked_idx.append(pool.pop())
        i += 1

    picked = df.loc[picked_idx].sort_values("question_id")

    with (TASKS_DIR / "mmlu_pro_50.jsonl").open("w") as f:
        for _, row in picked.iterrows():
            letters = [chr(ord("A") + i) for i in range(len(row["options"]))]
            f.write(json.dumps({
                "task_id": int(row["question_id"]),
                "question": row["question"],
                "options": list(row["options"]),
                "letters": letters,
                "answer_letter": row["answer"],
                "category": row["category"],
            }) + "\n")
    print(f"MMLU-Pro: sampled {len(picked)} across "
          f"{picked['category'].nunique()} subjects, seed={SEED}")
    print(picked["category"].value_counts().to_dict())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpora", type=Path, required=True,
                    help="directory holding the three source corpus files")
    ap.add_argument("--family", choices=["gsm8k", "mbpp", "mmlu_pro", "all"],
                    default="all")
    args = ap.parse_args()

    if args.family in ("gsm8k", "all"):
        build_gsm8k(args.corpora)
    if args.family in ("mbpp", "all"):
        build_mbpp(args.corpora)
    if args.family in ("mmlu_pro", "all"):
        build_mmlu_pro(args.corpora)


if __name__ == "__main__":
    main()
