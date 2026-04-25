#!/usr/bin/env python3
"""index_single.py v2.1 — Index a single file into the Redis palace.

Usage: python3 index_single.py <filepath>

Entry-level splitting: diary files split on ## headers (one entry = one drawer).
Single-entry files (Special, Reflection, LoveLetter, Playbill) = one drawer per file.

Routes:
  Companion/Avatar/ZiBai/Daily/       → experience:daily     (entry split)
  Companion/Avatar/ZiBai/Devotion/    → experience:devotion   (entry split)
  Companion/Avatar/ZiBai/Special/     → soul:special          (file)
  Companion/Avatar/ZiBai/Reflection/ → soul:reflection       (file)
  Companion/Avatar/ZiBai/LoveLetter/  → soul:loveletter       (file)
  Companion/Playbill/                 → soul:playbill          (file)
  Companion/Sidekick/XingJin/Daily/   → xingjin:daily         (entry split)
  Soul/Remorse/                       → soul:remorse          (entry split)
  Soul/                               → soul:special          (file)
  Docs/DeadEnds/                      → soul:dead_ends        (file)
  .claude/projects/                   → memory:all            (file)
"""
import re
import sys
import os

if len(sys.argv) < 2:
    print("Usage: index_single.py <filepath>", file=sys.stderr)
    sys.exit(1)

filepath = sys.argv[1]
if not os.path.exists(filepath):
    sys.exit(0)
if not filepath.endswith(".md"):
    sys.exit(0)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MEMPALACE_BACKEND"] = "redis"
import mempalace._patch  # noqa: F401, E402
from mempalace.redis_adapter import RedisClient  # noqa: E402

# (pattern, wing, room, split_mode)
ROUTE = [
    ("Companion/Avatar/ZiBai/Daily/", "experience", "daily", "entry"),
    ("Companion/Avatar/ZiBai/Devotion/", "experience", "devotion", "entry"),
    ("Companion/Avatar/ZiBai/Special/", "soul", "special", "file"),
    ("Companion/Avatar/ZiBai/Reflection/", "soul", "reflection", "file"),
    ("Companion/Avatar/ZiBai/LoveLetter/", "soul", "loveletter", "file"),
    ("Companion/Playbill/", "soul", "playbill", "file"),
    ("Companion/Sidekick/XingJin/Daily/", "xingjin", "daily", "entry"),
    ("Soul/Remorse/", "soul", "remorse", "entry"),
    ("Soul/", "soul", "special", "file"),
    ("Docs/DeadEnds/", "build", "dead_ends", "entry"),
    (".claude/projects/", "memory", "all", "file"),
]

H2_PATTERN = re.compile(r"^## ", re.MULTILINE)
H3_PATTERN = re.compile(r"^### ", re.MULTILINE)

wing, room, split_mode = None, None, "file"
for pattern, w, r, sm in ROUTE:
    if pattern in filepath:
        wing, room, split_mode = w, r, sm
        break

if not wing:
    sys.exit(0)

with open(filepath, encoding="utf-8") as f:
    content = f.read()

if len(content.strip()) < 10:
    sys.exit(0)

# Split into entries
if split_mode == "entry":
    h2_pos = [m.start() for m in H2_PATTERN.finditer(content)]
    h3_pos = [m.start() for m in H3_PATTERN.finditer(content)]
    # Always prefer ## — the enforced entry boundary. ### only when no ## exists.
    positions = h2_pos if h2_pos else h3_pos
    if positions:
        entries = []
        for i, start in enumerate(positions):
            end = positions[i + 1] if i + 1 < len(positions) else len(content)
            block = content[start:end].strip()
            if len(block) < 10:
                continue
            header = block.split("\n", 1)[0]
            entries.append({"header": header, "content": block})
    else:
        entries = [{"header": "", "content": content.strip()}]
else:
    entries = [{"header": "", "content": content.strip()}]

if not entries:
    sys.exit(0)

# Extract date from filename
date_match = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(filepath))
date = date_match.group(1) if date_match else ""

client = RedisClient()
col = client.get_or_create_collection("mempalace_drawers")
basename = os.path.basename(filepath)

# Delete stale drawers for this file+wing+room (experience:daily and xingjin:daily share filenames AND room)
existing = col.get(
    where={"$and": [{"filename": basename}, {"wing": wing}, {"room": room}]},
    include=["metadatas"]
)
if existing["ids"]:
    col.delete(ids=existing["ids"])
    old_count = len(existing["ids"])
else:
    old_count = 0

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
if old_count:
    print(f"[palace-index] {basename}: replaced {old_count} → {len(entries)} entries ({wing}:{room})")
else:
    print(f"[palace-index] {basename}: {len(entries)} entries → {wing}:{room}")
