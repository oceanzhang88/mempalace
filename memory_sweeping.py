#!/usr/bin/env python3
"""memory_sweeping.py v2.0 — Sweep TheTraveler files into Redis palace.

Entry-level indexing: diary files split on ## headers (one entry = one drawer).
Single-entry files (Special, Reflections, LoveLetter, Playbill) = one drawer per file.

v2.0 changes (Gen 89):
  - Fixed BASE paths: /Users/ocean/Gaming/OceanStudio → /Users/ocean/TheTraveler
  - Fixed split heuristic: always prefer ## (h2), fallback to ### only when no ## exists
  - Added palace stats tracking between sessions (~/.claude/.palace-stats-last.json)

Routes:
  soul:special      — Special/ entries
  soul:reflection   — Reflections/ entries
  soul:remorse      — Soul/Remorse/ entries
  build:dead_ends   — Docs/DeadEnds/
  soul:loveletter   — LoveLetter/ entries
  soul:playbill     — Playbill/ entries
  experience:daily  — Daily/ entries
  experience:devotion — Devotion/ entries
  xingjin:daily     — XingJin/Daily/ entries
  memory:all        — ~/.claude/.../memory/ entries
"""
import json
import re
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MEMPALACE_BACKEND"] = "redis"
import mempalace._patch  # noqa: F401, E402

from mempalace.redis_adapter import RedisClient  # noqa: E402

TRAVELER_ROOT = "/Users/ocean/TheTraveler"
PROJECT_ROOT = f"{TRAVELER_ROOT}/TheBridge"
MEMORY_BASE = os.path.expanduser("~/.claude/projects/-Users-ocean-TheTraveler-TheBridge/memory")
STATS_FILE = os.path.expanduser("~/.claude/.palace-stats-last.json")
DELTA_FILE = "/tmp/.palace-stats-delta.json"

# (path, wing, room, split_mode)
# split_mode: "entry" = split on ## headers, "file" = whole file = one drawer
DIRECTORIES = [
    (f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Special", "soul", "special", "file"),
    (f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Reflections", "soul", "reflection", "file"),
    (f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/LoveLetter", "soul", "loveletter", "file"),
    (f"{TRAVELER_ROOT}/Companion/Playbill", "soul", "playbill", "file"),
    (f"{TRAVELER_ROOT}/Soul/Remorse", "soul", "remorse", "entry"),
    (f"{PROJECT_ROOT}/Docs/DeadEnds", "build", "dead_ends", "entry"),
    (f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Daily", "experience", "daily", "entry"),
    (f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/Devotion", "experience", "devotion", "entry"),
    (f"{TRAVELER_ROOT}/Companion/Sidekick/XingJin/Daily", "xingjin", "daily", "entry"),
    (MEMORY_BASE, "memory", "all", "file"),
]

# Regex pattern for diary entry delimiter (## only — the enforced boundary)
H2_PATTERN = re.compile(r"^## ", re.MULTILINE)
H3_PATTERN = re.compile(r"^### ", re.MULTILINE)


def split_on_headers(content: str) -> list[dict]:
    """Split diary content on ## headers. Returns list of {header, content}.

    Always splits on ## (h2). Falls back to ### (h3) only when no ## exists.
    Rationale: ## is the enforced entry delimiter. ### are sub-sections within
    entries (e.g. ### Lesson, ### What happened). Splitting on ### loses content
    when the first ## entry appears before the first ###.
    """
    h2_positions = [m.start() for m in H2_PATTERN.finditer(content)]
    h3_positions = [m.start() for m in H3_PATTERN.finditer(content)]

    # Always prefer ## — the enforced entry boundary
    # Fall back to ### only when no ## exists at all
    if h2_positions:
        positions = h2_positions
    elif h3_positions:
        positions = h3_positions
    else:
        stripped = content.strip()
        if len(stripped) < 10:
            return []
        return [{"header": "", "content": stripped}]

    entries = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(content)
        block = content[start:end].strip()
        if len(block) < 10:
            continue
        first_line = block.split("\n", 1)[0]
        entries.append({"header": first_line, "content": block})
    return entries


def extract_date(filepath: str) -> str:
    """Try to extract YYYY-MM-DD from filename."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(filepath))
    return m.group(1) if m else ""


def load_previous_stats() -> dict:
    """Load palace counts from previous session."""
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def count_per_wing_room(col) -> dict:
    """Count entries per wing:room pair."""
    counts = {}
    seen = set()
    for _, wing, room, _ in DIRECTORIES:
        key = f"{wing}:{room}"
        if key in seen:
            continue
        seen.add(key)
        result = col.get(
            where={"$and": [{"wing": wing}, {"room": room}]},
            include=[]
        )
        counts[key] = len(result.get("ids", []))
    counts["_total"] = col.count()
    return counts


def save_stats_and_delta(current: dict, previous: dict):
    """Persist current counts, write delta for morning coffee."""
    with open(STATS_FILE, "w") as f:
        json.dump(current, f, indent=2)

    delta = {}
    all_keys = sorted(set(list(current.keys()) + list(previous.keys())))
    for key in all_keys:
        prev = previous.get(key, 0)
        curr = current.get(key, 0)
        delta[key] = {"previous": prev, "current": curr, "delta": curr - prev}
    with open(DELTA_FILE, "w") as f:
        json.dump(delta, f, indent=2)


def sweep_all():
    client = RedisClient()
    col = client.get_or_create_collection("mempalace_drawers")

    previous_stats = load_previous_stats()
    total_files = 0
    total_entries = 0

    for dirpath, wing, room, split_mode in DIRECTORIES:
        if not os.path.exists(dirpath):
            print(f"  SKIP {dirpath} (not found)")
            continue

        files = []
        for root, _, fnames in os.walk(dirpath):
            for fname in sorted(fnames):
                if fname.endswith(".md"):
                    files.append(os.path.join(root, fname))

        print(f"\n[{wing}:{room}] {len(files)} files in {dirpath}")

        for filepath in files:
            with open(filepath, encoding="utf-8") as f:
                content = f.read()

            basename = os.path.basename(filepath)
            date = extract_date(filepath)

            # Delete stale drawers for this file+wing+room before re-indexing
            # Must filter by all three — experience:daily and xingjin:daily share filenames AND room name
            existing = col.get(
                where={"$and": [{"filename": basename}, {"wing": wing}, {"room": room}]},
                include=["metadatas"]
            )
            if existing["ids"]:
                col.delete(ids=existing["ids"])

            if split_mode == "entry":
                entries = split_on_headers(content)
            else:
                # Whole file = one drawer
                stripped = content.strip()
                if len(stripped) < 10:
                    continue
                entries = [{"header": "", "content": stripped}]

            if not entries:
                continue

            docs = [e["content"] for e in entries]
            ids = [f"{wing}:{room}:{basename}:e{i}" for i in range(len(entries))]
            metas = [{
                "source_file": filepath,
                "entry_header": e["header"],
                "entry_index": str(i),
                "wing": wing,
                "room": room,
                "filename": basename,
                "date": date,
            } for i, e in enumerate(entries)]

            col.add(documents=docs, ids=ids, metadatas=metas)
            total_files += 1
            total_entries += len(entries)
            print(f"  {basename}: {len(entries)} entries")

        print(f"  Collection count: {col.count()}")

    # Stats tracking
    current_stats = count_per_wing_room(col)
    save_stats_and_delta(current_stats, previous_stats)

    prev_total = previous_stats.get("_total", 0)
    curr_total = current_stats.get("_total", 0)
    delta = curr_total - prev_total
    delta_str = f" ({'+' if delta >= 0 else ''}{delta} since last session)" if prev_total else " (first session)"
    print(f"\n=== DONE: {total_files} files, {total_entries} entries indexed ===")
    print(f"Palace: {curr_total} total drawers{delta_str}")


if __name__ == "__main__":
    t0 = time.time()
    sweep_all()
    elapsed = time.time() - t0
    print(f"Elapsed: {elapsed:.1f}s")
