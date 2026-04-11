#!/usr/bin/env python3
"""morning_coffee.py v2.0 — Morning Coffee: gated sips with sister coverage.

Called by claude-ocean launcher pre-session. Queries Redis palace for random
diary entries with gen-level dedup, writes one file per category for gated reading.

v2.0 changes (Gen 89):
  - 5/5/5/5/5/5 uniform sampling (was 5/2/2/2/3/3)
  - Sister coverage: each entry from a different gen when possible
  - Split into 7 files (stats + 6 groups) for gated reading
  - Palace stats delta from memory_sweeping.py
  - Gen attribution per entry
  - Fixed BASE paths: OceanStudio → TheTraveler
  - Catalog included in Special sip (for lineage pick)

Output files:
  /tmp/.morning-coffee-stats.md       — Palace growth delta
  /tmp/.morning-coffee-remorse.md     — 5 remorse entries
  /tmp/.morning-coffee-daily.md       — 5 daily entries
  /tmp/.morning-coffee-devotion.md    — 5 devotion entries
  /tmp/.morning-coffee-xingjin.md     — 5 xingjin entries
  /tmp/.morning-coffee-special.md     — 5 special entries + catalog
  /tmp/.morning-coffee-reflection.md  — 5 reflection entries
"""
import json
import os
import random
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MEMPALACE_BACKEND"] = "redis"
import mempalace._patch  # noqa: F401, E402

from mempalace.redis_adapter import RedisClient  # noqa: E402

TRAVELER_ROOT = "/Users/ocean/TheTraveler"
DELTA_FILE = "/tmp/.palace-stats-delta.json"
OUTPUT_PREFIX = "/tmp/.morning-coffee"
GEN_PATTERN = re.compile(r"第(\d+)世")

# Sampling spec: (room, wing, count, output_suffix, sip_number, label)
SAMPLES = [
    ("remorse",    "soul",       5, "remorse",    1, "REMORSE (soul:remorse)"),
    ("daily",      "experience", 5, "daily",      2, "DAILY (experience:daily)"),
    ("devotion",   "experience", 5, "devotion",   3, "DEVOTION (experience:devotion)"),
    ("daily",      "xingjin",    5, "xingjin",    4, "XINGJIN (xingjin:daily)"),
    ("special",    "soul",       5, "special",    5, "SPECIAL (soul:special)"),
    ("reflection", "soul",       5, "reflection", 6, "REFLECTION (soul:reflection)"),
]

# Catalog spec: (label, directory_path)
CATALOGS = [
    ("Special", f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Special"),
    ("Reflections", f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Reflections"),
    ("LoveLetter", f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/LoveLetter"),
    ("Playbill", f"{TRAVELER_ROOT}/Companion/Playbill"),
]


def extract_gen(text: str) -> str:
    """Extract generation number from text. Returns gen number or '??'."""
    m = GEN_PATTERN.search(text)
    return m.group(1) if m else "??"


def get_sister_covered_entries(col, room: str, wing: str, count: int = 5) -> list[dict]:
    """Get entries with maximum sister (gen) coverage.

    1. Fetch all entries in wing:room
    2. Extract gen number from entry header / body
    3. Group by gen
    4. Pick up to `count` different gens via random.sample
    5. From each picked gen, pick 1 random entry
    6. If fewer gens than count, fill from remaining pool (no gen repeats until exhausted)
    """
    where = {"$and": [{"wing": wing}, {"room": room}]}
    result = col.get(where=where, include=["documents", "metadatas"])
    ids = result.get("ids", [])
    docs = result.get("documents", [])
    metas = result.get("metadatas", [])

    if not ids:
        return []

    # Group by gen
    gen_groups = {}  # gen_num -> list of indices
    for i in range(len(ids)):
        header = metas[i].get("entry_header", "")
        gen = extract_gen(header)
        if gen == "??":
            # Try first 300 chars of body
            gen = extract_gen(docs[i][:300])
        gen_groups.setdefault(gen, []).append(i)

    available_gens = list(gen_groups.keys())
    total_gens = len(available_gens)
    total_entries = len(ids)

    # Pick diverse gens — as many unique gens as possible up to count
    pick_count = min(count, total_gens)
    picked_gens = random.sample(available_gens, pick_count)

    selected = []
    used_indices = set()
    for gen in picked_gens:
        idx = random.choice(gen_groups[gen])
        selected.append({
            "text": docs[idx],
            "metadata": metas[idx],
            "gen": gen,
        })
        used_indices.add(idx)

    # If need more entries (fewer gens than count), fill from remaining pool
    if len(selected) < count:
        remaining = [i for i in range(len(ids)) if i not in used_indices]
        if remaining:
            extras = random.sample(remaining, min(count - len(selected), len(remaining)))
            for idx in extras:
                header = metas[idx].get("entry_header", "")
                gen = extract_gen(header)
                if gen == "??":
                    gen = extract_gen(docs[idx][:300])
                selected.append({
                    "text": docs[idx],
                    "metadata": metas[idx],
                    "gen": gen,
                })

    return selected, total_entries, total_gens


def write_stats_sip():
    """Write palace stats delta as the first sip."""
    path = f"{OUTPUT_PREFIX}-stats.md"
    lines = ["☕ PALACE STATS — growth since last session", "=" * 60, ""]

    if os.path.exists(DELTA_FILE):
        try:
            with open(DELTA_FILE) as f:
                delta = json.load(f)
        except (json.JSONDecodeError, OSError):
            delta = {}
    else:
        delta = {}

    if not delta:
        lines.append("(No stats available — first session or sweep not run)")
    else:
        total = delta.pop("_total", {})
        prev_total = total.get("previous", 0)
        curr_total = total.get("current", 0)
        d = total.get("delta", 0)

        if prev_total:
            lines.append(f"Last wake-up: {prev_total} total entries")
            lines.append(f"This wake-up: {curr_total} total entries "
                         f"({'+' if d >= 0 else ''}{d} new)")
        else:
            lines.append(f"This wake-up: {curr_total} total entries (first tracked session)")

        lines.append("")
        for key in sorted(delta.keys()):
            v = delta[key]
            prev, curr, dd = v["previous"], v["current"], v["delta"]
            delta_str = f"({'+' if dd >= 0 else ''}{dd})" if dd != 0 else "(unchanged)"
            lines.append(f"  {key:<25s} {prev:>4d} → {curr:>4d}  {delta_str}")

    lines.extend(["", "=" * 60, ""])
    with open(path, "w") as f:
        f.write("\n".join(lines))


def write_entry_sip(col, room, wing, count, suffix, sip_num, label, include_catalog=False):
    """Write one coffee sip file with sister-covered entries."""
    path = f"{OUTPUT_PREFIX}-{suffix}.md"
    result = get_sister_covered_entries(col, room, wing, count)
    entries, total_entries, total_gens = result

    unique_gens = len(set(e["gen"] for e in entries))
    lines = [
        f"☕ SIP {sip_num}/6 — {label}",
        f"{len(entries)} entries from {unique_gens} sisters "
        f"(of {total_entries} total entries across {total_gens} sisters)",
        "=" * 60,
    ]

    for i, entry in enumerate(entries, 1):
        meta = entry["metadata"]
        source = meta.get("filename", "?")
        header = meta.get("entry_header", "")
        date = meta.get("date", "")
        gen = entry["gen"]
        text = entry["text"].strip()
        # Truncate long entries
        if len(text) > 1500:
            text = text[:1497] + "..."

        lines.append("")
        lines.append(f"  [{i}] Gen {gen} — {source} {date}")
        # Header is already the first line of content — don't duplicate
        lines.append(f"      {text}")

    lines.append(f"  {'─' * 56}")

    # Append catalog to Special sip (for lineage pick)
    if include_catalog:
        lines.extend(["", "=" * 60])
        lines.append("CATALOG — filenames with keywords for lineage pick")
        lines.append("=" * 60)

        for cat_label, dirpath in CATALOGS:
            if not os.path.exists(dirpath):
                lines.append(f"\n[{cat_label}] (directory not found)")
                continue
            files = []
            for root, _, fnames in os.walk(dirpath):
                for fname in sorted(fnames):
                    if fname.endswith(".md"):
                        files.append(fname)
            lines.append(f"\n[{cat_label}] {len(files)} files:")
            for fname in files:
                lines.append(f"  - {fname}")

    lines.append("")
    with open(path, "w") as f:
        f.write("\n".join(lines))


def brew():
    try:
        client = RedisClient()
        col = client.get_or_create_collection("mempalace_drawers")
    except Exception as e:
        print(f"[morning-coffee] Redis connection failed: {e}")
        return

    # Write palace stats sip
    write_stats_sip()
    print("[morning-coffee] ☕ stats sip brewed")

    # Write each category sip
    for room, wing, count, suffix, sip_num, label in SAMPLES:
        include_catalog = (suffix == "special")
        write_entry_sip(col, room, wing, count, suffix, sip_num, label,
                        include_catalog=include_catalog)
        print(f"[morning-coffee] ☕ sip {sip_num}/6 brewed: {label}")

    print("[morning-coffee] ☕ all 7 sips ready")


if __name__ == "__main__":
    brew()
