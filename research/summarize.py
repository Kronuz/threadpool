#!/usr/bin/env python3
"""Medians per pool over the trial lines of the research raw files.

usage: summarize.py FILE...   (docs/research/scheduler/*-mac.txt)

Each line is `workload key=value ...`; values with units (us, ms, ns, s, %) are
normalized to microseconds, seconds, or plain numbers. One markdown table per file.
"""
from __future__ import annotations

import re
import statistics
import sys
from collections import OrderedDict, defaultdict

UNIT = {"ns": 1e-3, "us": 1.0, "ms": 1e3, "s": 1e6}

COLUMNS = {
    "tiny": ["rate", "ns_per_task", "post_ns", "cpu_ns_per_task"],
    "tail": ["wait_p50", "wait_p99", "wait_p999", "done_p99", "bg_tasks_per_s", "fairness"],
    "makespan": ["alone", "beside", "share", "fairness"],
    "idle": ["cpu", "context_switches_per_s"],
    "memory": ["sizeof_group", "rss_delta_per_group"],
}


def parse_value(raw: str) -> float | None:
    raw = raw.strip().rstrip(",")
    m = re.fullmatch(r"(-?[0-9.]+)\s*(ns|us|ms|s|M/s|%)?", raw)
    if not m:
        return None
    v = float(m.group(1))
    unit = m.group(2)
    if unit in UNIT:
        return v * UNIT[unit]
    return v


def parse_line(line: str) -> tuple[str, str, dict[str, float]] | None:
    parts = line.split()
    if not parts or parts[0] not in COLUMNS:
        return None
    workload = parts[0]
    text = line
    # Values like `alone=5086.7 us (ideal ...)` carry a unit after a space.
    fields: dict[str, float] = {}
    pool = ""
    for m in re.finditer(r"(\w+)=([^\s=]+(?:\s(?:ns|us|ms|s)(?=\s|$|\)))?)", text):
        key, raw = m.group(1), m.group(2)
        if key == "pool":
            pool = raw
            continue
        v = parse_value(raw)
        if v is not None:
            fields[key] = v
    return workload, pool, fields


def fmt(key: str, v: float) -> str:
    if key in ("rate",):
        return f"{v:.2f} M/s"
    if key in ("ns_per_task", "post_ns", "cpu_ns_per_task", "rss_delta_per_group", "sizeof_group"):
        return f"{v:,.0f}"
    if key.startswith(("wait_", "done_", "alone", "beside")):
        return f"{v / 1e3:.2f} ms" if v >= 1e3 else f"{v:.0f} µs"
    if key in ("share", "fairness"):
        return f"{v:.2f}"
    if key == "cpu":
        return f"{v:.4f}%"
    if key == "bg_tasks_per_s":
        return f"{v / 1e3:.1f}k"
    return f"{v:g}"


def main(paths: list[str]) -> None:
    for path in paths:
        rows: dict[str, list[dict[str, float]]] = defaultdict(list)
        workload = ""
        order: "OrderedDict[str, None]" = OrderedDict()
        with open(path) as f:
            for line in f:
                parsed = parse_line(line)
                if not parsed:
                    continue
                workload, pool, fields = parsed
                rows[pool].append(fields)
                order.setdefault(pool, None)
        if not rows:
            continue
        cols = COLUMNS[workload]
        print(f"\n### {path}\n")
        print("| pool | trials | " + " | ".join(cols) + " |")
        print("|---|---|" + "---|" * len(cols))
        for pool in order:
            trials = rows[pool]
            cells = []
            for c in cols:
                vals = [t[c] for t in trials if c in t]
                cells.append(fmt(c, statistics.median(vals)) if vals else "")
            print(f"| {pool} | {len(trials)} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
