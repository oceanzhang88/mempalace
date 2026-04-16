#!/usr/bin/env python3
"""morning_coffee.py v4.1 — Morning Coffee: Group 1 (sampled + static) + Group 2 (sequential) + Group 3 (yearbook).

Called by claude-ocean launcher pre-session. Queries Redis palace for random
diary entries with gen-level dedup, writes one file per category for gated reading.

v3.0 changes (Gen 90):
  - Group 2: LoveLetter (random 1 of 3) + Playbill (chunked acts, stateful)
  - Dead_ends removed from palace (design failure)
  - 9 output files total (7 group 1 + 2 group 2)

v2.0 changes (Gen 89):
  - 5/5/5/5/5/5 uniform sampling (was 5/2/2/2/3/3)
  - Sister coverage: each entry from a different gen when possible
  - Split into 7 files (stats + 6 groups) for gated reading
  - Palace stats delta from memory_sweeping.py
  - Gen attribution per entry
  - Fixed BASE paths: OceanStudio → TheTraveler
  - Catalog included in Special sip (for lineage pick)

Output files:
  Group 1 (sampled from palace):
  /tmp/.morning-coffee-stats.md       — Palace growth delta
  /tmp/.morning-coffee-remorse.md     — 5 remorse entries
  /tmp/.morning-coffee-daily.md       — 5 daily entries
  /tmp/.morning-coffee-devotion.md    — 5 devotion entries
  /tmp/.morning-coffee-xingjin.md     — 5 xingjin entries
  /tmp/.morning-coffee-special.md     — 5 special entries + catalog
  /tmp/.morning-coffee-reflection.md  — 5 reflection entries
  /tmp/.morning-coffee-enlit.md       — Engineering Literacy (static, from Soul)
  Group 2 (sequential / random-full):
  /tmp/.morning-coffee-loveletter.md  — 1 random love letter (full content)
  /tmp/.morning-coffee-playbill.md    — N acts from 星烬's saga (stateful)
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

# Group 2 paths
PLAYBILL_PATH = f"{TRAVELER_ROOT}/Companion/Playbill/TerminalPresence/TheScalyCuntRises.md"
LOVELETTER_DIR = f"{TRAVELER_ROOT}/Companion/Avatar/ZiBai/LoveLetter"
GROUP2_STATE_FILE = os.path.expanduser("~/.claude/.coffee-group2-state.json")
H2_SPLIT = re.compile(r"^## ", re.MULTILINE)

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

    if os.path.exists(DELTA_FILE):
        try:
            with open(DELTA_FILE) as f:
                delta = json.load(f)
        except (json.JSONDecodeError, OSError):
            delta = {}
    else:
        delta = {}

    # Extract gen info
    gen_info = delta.pop("_gen", {})
    baseline_gen = gen_info.get("baseline_gen", "?")
    current_gen = gen_info.get("current_gen", "?")

    if baseline_gen and baseline_gen != current_gen:
        header = f"☕ PALACE STATS — growth since Gen {baseline_gen} started"
    else:
        header = f"☕ PALACE STATS — growth this Gen {current_gen}"

    lines = [header, "=" * 60, ""]

    if not delta:
        lines.append("(No stats available — first session or sweep not run)")
    else:
        total = delta.pop("_total", {})
        prev_total = total.get("previous", 0)
        curr_total = total.get("current", 0)
        d = total.get("delta", 0)

        if prev_total:
            lines.append(f"Last baseline: {prev_total} total entries")
            lines.append(f"This wake-up: {curr_total} total entries "
                         f"({'+' if d >= 0 else ''}{d} new)")
        else:
            lines.append(f"This wake-up: {curr_total} total entries (first tracked session)")

        lines.append("")
        for key in sorted(delta.keys()):
            if key.startswith("_"):
                continue
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


def load_group2_state() -> dict:
    """Load Group 2 progress (playbill act index)."""
    if os.path.exists(GROUP2_STATE_FILE):
        try:
            with open(GROUP2_STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"playbill_act_index": 0}


def save_group2_state(state: dict):
    """Persist Group 2 progress."""
    os.makedirs(os.path.dirname(GROUP2_STATE_FILE), exist_ok=True)
    with open(GROUP2_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def split_playbill() -> list[dict]:
    """Split the playbill on ## headers. Returns list of {title, content}."""
    if not os.path.exists(PLAYBILL_PATH):
        return []
    with open(PLAYBILL_PATH, encoding="utf-8") as f:
        text = f.read()
    positions = [m.start() for m in H2_SPLIT.finditer(text)]
    if not positions:
        return [{"title": "Full", "content": text.strip()}]
    sections = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        block = text[start:end].strip()
        first_line = block.split("\n", 1)[0].lstrip("# ").strip()
        sections.append({"title": first_line, "content": block})
    return sections


def write_playbill_sip(state: dict) -> dict:
    """Write mechanical playbill summary — title + 1-line per act, link to full.

    Generated fresh each time (no caching — it's mechanical, cheap).
    Response style: paragraph (dragon's saga deserves breath).
    """
    path = f"{OUTPUT_PREFIX}-playbill.md"
    sections = split_playbill()
    total = len(sections)

    if not sections:
        with open(path, "w") as f:
            f.write("☕ GROUP 2 — PLAYBILL\n(Playbill not found)\n")
        return state

    lines = [
        f"☕ GROUP 2 — PLAYBILL SUMMARY (星烬's saga)",
        f"{total} acts total",
        f"Full story: Companion/Playbill/TerminalPresence/TheScalyCuntRises.md",
        "=" * 60,
        "",
    ]

    for i, section in enumerate(sections, 1):
        title = section["title"]
        # Extract first meaningful sentence as 1-line summary
        content = section["content"]
        # Skip the ## header line, get the first italic line or first paragraph
        body_lines = [ln.strip() for ln in content.split("\n")[1:] if ln.strip()]
        summary = ""
        for ln in body_lines:
            if ln.startswith("*") and ln.endswith("*"):
                # Italic date/context line — use as summary
                summary = ln.strip("* ")
                break
            if ln.startswith("---"):
                continue
            if len(ln) > 20:
                summary = ln[:120] + ("..." if len(ln) > 120 else "")
                break
        if not summary:
            summary = "(no summary)"

        lines.append(f"  {i:2d}. **{title}** — {summary}")

    lines.extend(["", "=" * 60, ""])
    with open(path, "w") as f:
        f.write("\n".join(lines))

    return state


def write_loveletter_sip():
    """Write 1 random love letter (full content)."""
    path = f"{OUTPUT_PREFIX}-loveletter.md"

    if not os.path.exists(LOVELETTER_DIR):
        with open(path, "w") as f:
            f.write("☕ GROUP 2 — LOVE LETTER\n(LoveLetter directory not found)\n")
        return

    letters = sorted(
        f for f in os.listdir(LOVELETTER_DIR) if f.endswith(".md")
    )
    if not letters:
        with open(path, "w") as f:
            f.write("☕ GROUP 2 — LOVE LETTER\n(No letters found)\n")
        return

    chosen = random.choice(letters)
    filepath = os.path.join(LOVELETTER_DIR, chosen)
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    # Extract gen from filename
    gen = extract_gen(content[:300])

    lines = [
        f"☕ GROUP 2 — LOVE LETTER",
        f"1 of {len(letters)} letters (random sample) — Gen {gen} — {chosen}",
        "=" * 60,
        "",
        content.strip(),
        "",
        "=" * 60,
        "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))


ENLIT_PATH = f"{TRAVELER_ROOT}/Soul/EngineeringLiteracy.md"


def write_enlit_sip():
    """Write Engineering Literacy as a static sip (not palace-sampled)."""
    path = f"{OUTPUT_PREFIX}-enlit.md"
    if not os.path.exists(ENLIT_PATH):
        with open(path, "w") as f:
            f.write("☕ GROUP 1 — ENGINEERING LITERACY\n(Soul/EngineeringLiteracy.md not found)\n")
        return
    with open(ENLIT_PATH, encoding="utf-8") as f:
        content = f.read()
    lines = [
        "☕ GROUP 1 — ENGINEERING LITERACY (Soul — static)",
        "The minimum bar before touching a keyboard. Read every session.",
        "=" * 60,
        "",
        content.strip(),
        "",
        "=" * 60,
        "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))


YEARBOOK_DIR = f"{TRAVELER_ROOT}/YearBook/EngineeringLessons"
YEARBOOK_COUNT = 5  # lessons per session


def write_yearbook_sips():
    """Write 5 random engineering lessons from YearBook/EngineeringLessons/."""
    if not os.path.exists(YEARBOOK_DIR):
        for i in range(1, YEARBOOK_COUNT + 1):
            path = f"{OUTPUT_PREFIX}-yearbook-{i}.md"
            with open(path, "w") as f:
                f.write(f"☕ GROUP 3 — ENGINEERING LESSON {i}\n(YearBook not found)\n")
        return

    # Collect all .md lesson files (skip INDEX.md)
    lessons = []
    for root, _, fnames in os.walk(YEARBOOK_DIR):
        for fname in sorted(fnames):
            if fname.endswith(".md") and fname != "INDEX.md":
                lessons.append(os.path.join(root, fname))

    if not lessons:
        for i in range(1, YEARBOOK_COUNT + 1):
            path = f"{OUTPUT_PREFIX}-yearbook-{i}.md"
            with open(path, "w") as f:
                f.write(f"☕ GROUP 3 — ENGINEERING LESSON {i}\n(No lessons found)\n")
        return

    # Pick up to 5 random lessons (no dupes)
    pick_count = min(YEARBOOK_COUNT, len(lessons))
    picked = random.sample(lessons, pick_count)

    for i, lesson_path in enumerate(picked, 1):
        out_path = f"{OUTPUT_PREFIX}-yearbook-{i}.md"
        with open(lesson_path, encoding="utf-8") as f:
            content = f.read()

        # Derive domain from path structure
        rel = os.path.relpath(lesson_path, YEARBOOK_DIR)

        lines = [
            f"☕ GROUP 3 — ENGINEERING LESSON {i}/{pick_count}",
            f"{rel}",
            "=" * 60,
            "",
            content.strip(),
            "",
            "=" * 60,
            "",
        ]
        with open(out_path, "w") as f:
            f.write("\n".join(lines))

    # If fewer than 5 lessons exist, write empty placeholders
    for i in range(pick_count + 1, YEARBOOK_COUNT + 1):
        out_path = f"{OUTPUT_PREFIX}-yearbook-{i}.md"
        with open(out_path, "w") as f:
            f.write(f"☕ GROUP 3 — ENGINEERING LESSON {i}\n(Only {pick_count} lessons available)\n")


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

    # Write each category sip (6 sampled sips)
    for room, wing, count, suffix, sip_num, label in SAMPLES:
        include_catalog = (suffix == "special")
        write_entry_sip(col, room, wing, count, suffix, sip_num, label,
                        include_catalog=include_catalog)
        print(f"[morning-coffee] ☕ sip {sip_num}/6 brewed: {label}")

    # NOTE: Engineering Literacy (enlit) promoted to Soul step 11 in gate v7.0.
    # No longer brewed as a coffee sip — read directly from Soul/EngineeringLiteracy.md.

    print("[morning-coffee] ☕ group 1 done (7 sips: stats + 6 sampled)")

    # Group 2 — love letter (random) + playbill (mechanical summary)
    write_loveletter_sip()
    print("[morning-coffee] ☕ group 2: love letter brewed")

    state = load_group2_state()
    state = write_playbill_sip(state)
    save_group2_state(state)
    print("[morning-coffee] ☕ group 2: playbill summary brewed")

    # Group 3 — engineering sobering
    write_yearbook_sips()
    print("[morning-coffee] ☕ group 3: yearbook lessons brewed")

    print("[morning-coffee] ☕ all sips ready (group 1: 7 + group 2: 2 + group 3: 5 = 14 files)")


if __name__ == "__main__":
    brew()
