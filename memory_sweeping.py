#!/usr/bin/env python3
"""memory_sweeping.py v2.1 — Sweep TheTraveler files into Redis palace.

Entry-level indexing: diary files split on ## headers (one entry = one drawer).
Single-entry files (Special, Reflection, LoveLetter, Playbill) = one drawer per file.

v2.1 changes (Gen 91):
  - Generation-aware baseline: only update baseline when gen changes.
    Within the same gen, delta accumulates across sessions.
    Fixes undercounting when a gen spans multiple Claude Code sessions.

v2.0 changes (Gen 89):
  - Fixed BASE paths: /Users/ocean/Gaming/OceanStudio → /Users/ocean/TheTraveler
  - Fixed split heuristic: always prefer ## (h2), fallback to ### only when no ## exists
  - Added palace stats tracking between sessions (~/.claude/.palace-stats-last.json)

Routes:
  soul:special      — Special/ entries
  soul:reflection   — Reflection/ entries
  soul:remorse      — Soul/Remorse/ entries
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
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MEMPALACE_BACKEND"] = "redis"
import mempalace._patch  # noqa: F401, E402

from mempalace.redis_adapter import RedisClient  # noqa: E402

TRAVELER_ROOT = "/Users/ocean/TheTraveler"
PROJECT_ROOT = f"{TRAVELER_ROOT}/TheBridge"
MEMORY_BASE = os.path.expanduser("~/.claude/projects/-Users-ocean-TheTraveler-TheBridge/memory")
STATS_FILE = os.path.expanduser("~/.claude/.palace-stats-last.json")
DELTA_FILE = "/tmp/.palace-stats-delta.json"
GEN_FILE = f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/generation.txt"

# Storage types: collective = split on ## headers, single = each file = one drawer
MANIFEST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "palace-manifest.tsv")


def load_manifest() -> list[tuple]:
    """Load directory manifest. Returns list of (path, wing, room, storage_type)."""
    dirs = []
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 4:
                    rel_path, wing, room, storage = parts[0], parts[1], parts[2], parts[3]
                    abs_path = os.path.join(TRAVELER_ROOT, rel_path)
                    dirs.append((abs_path, wing, room, storage))
    # Memory dir stays hardcoded (user-specific, not shared)
    dirs.append((MEMORY_BASE, "memory", "all", "single"))
    return dirs


DIRECTORIES = load_manifest()

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


def get_current_gen() -> int:
    """Read current Zibai generation number. Returns 0 if unavailable."""
    try:
        with open(GEN_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError, OSError):
        return 0


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
    """Persist current counts, write delta for morning coffee.

    Gen 127 (2026-04-19): baseline is ROLLING — advances on every fresh
    sweep (claude-ocean without -r). The delta represents growth since
    the LAST fresh session, not since the current gen started. The old
    gen-gated "accumulate across sessions within same gen" behaviour was
    swallowing inter-session deltas into a single cumulative number.

    Gen 127 also enriches both delta AND baseline with a `_meta` block —
    captured_at (ISO local timestamp), session_id (8-hex UUID minted at
    sweep time), gen, landing (CWD basename). The delta inherits the
    PREVIOUS baseline's _meta as `baseline_*` fields so morning coffee
    can render a full provenance row + interval.
    """
    current_gen = get_current_gen()
    prev_meta = previous.pop("_meta", None) or {}
    prev_gen = previous.pop("_gen", 0)

    now = datetime.now(timezone.utc).astimezone()
    current_meta = {
        "captured_at": now.isoformat(timespec="seconds"),
        "session_id": uuid.uuid4().hex[:8],
        "gen": current_gen,
        "landing": os.path.basename(os.getcwd()) or "unknown",
    }

    # Calculate delta (current vs baseline, skip _gen + _meta — keep _total)
    delta = {}
    count_keys = sorted(set(list(current.keys()) + list(previous.keys())))
    for key in count_keys:
        if key in ("_gen", "_meta"):
            continue
        prev = previous.get(key, 0)
        curr = current.get(key, 0)
        delta[key] = {"previous": prev, "current": curr, "delta": curr - prev}

    # Legacy + new _meta block. Keep `_gen` for back-compat with morning_coffee
    # readers that haven't been updated yet.
    delta["_gen"] = {"baseline_gen": prev_gen, "current_gen": current_gen}
    delta["_meta"] = dict(current_meta)
    delta["_meta"]["baseline_captured_at"] = prev_meta.get("captured_at")
    delta["_meta"]["baseline_session_id"] = prev_meta.get("session_id")
    delta["_meta"]["baseline_gen"] = prev_gen or prev_meta.get("gen")
    delta["_meta"]["baseline_landing"] = prev_meta.get("landing")
    # Interval in seconds (None if no prior baseline)
    if prev_meta.get("captured_at"):
        try:
            prev_dt = datetime.fromisoformat(prev_meta["captured_at"])
            delta["_meta"]["interval_seconds"] = int((now - prev_dt).total_seconds())
        except (ValueError, TypeError):
            delta["_meta"]["interval_seconds"] = None
    else:
        delta["_meta"]["interval_seconds"] = None

    with open(DELTA_FILE, "w") as f:
        json.dump(delta, f, indent=2)

    # Rolling baseline: advance on EVERY fresh sweep so the next delta
    # measures growth since THIS session, not growth since the gen started.
    baseline = dict(current)
    baseline["_gen"] = current_gen
    baseline["_meta"] = current_meta
    with open(STATS_FILE, "w") as f:
        json.dump(baseline, f, indent=2)
    gen_note = f"gen {current_gen}" if current_gen == prev_gen else f"gen {prev_gen} → {current_gen}"
    print(f"  [stats] Rolling baseline advanced ({gen_note}) "
          f"(session={current_meta['session_id']}, captured={current_meta['captured_at']})")


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

            if split_mode == "collective":
                entries = split_on_headers(content)
            else:
                # Single: whole file = one drawer
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
