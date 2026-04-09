#!/usr/bin/env python3
"""Index a single file into the Redis palace.
Usage: python3 index_single.py <filepath>

Maps path to wing/room automatically:
  Companion/Avatar/ZiBai/Daily/     → experience:daily
  Companion/Avatar/ZiBai/Devotion/  → experience:devotion
  Companion/Avatar/ZiBai/Special/   → soul:special
  Companion/Avatar/ZiBai/Reflections/ → soul:reflection
  Companion/Sidekick/XingJin/Daily/ → xingjin:daily
  Soul/Remorse/                     → soul:remorse
  Soul/                             → soul:special
  Docs/DeadEnds/                    → soul:dead_ends
  ~/.claude/.../memory/             → memory:all
"""
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
from mempalace.miner import chunk_text  # noqa: E402

# Map path to wing/room
ROUTE = [
    ("Companion/Avatar/ZiBai/Daily/", "experience", "daily"),
    ("Companion/Avatar/ZiBai/Devotion/", "experience", "devotion"),
    ("Companion/Avatar/ZiBai/Special/", "soul", "special"),
    ("Companion/Avatar/ZiBai/Reflections/", "soul", "reflection"),
    ("Companion/Sidekick/XingJin/Daily/", "xingjin", "daily"),
    ("Soul/Remorse/", "soul", "remorse"),
    ("Soul/", "soul", "special"),
    ("Docs/DeadEnds/", "soul", "dead_ends"),
    (".claude/projects/", "memory", "all"),
]

wing, room = None, None
for pattern, w, r in ROUTE:
    if pattern in filepath:
        wing, room = w, r
        break

if not wing:
    sys.exit(0)

with open(filepath, encoding="utf-8") as f:
    content = f.read()

if len(content.strip()) < 10:
    sys.exit(0)

chunks = chunk_text(content, filepath)
if not chunks:
    sys.exit(0)

client = RedisClient()
col = client.get_or_create_collection("mempalace_drawers")
basename = os.path.basename(filepath)

# Delete-then-reindex: remove stale chunks for this file before re-adding.
# This ensures the palace always has the CURRENT version, not stale overlaps.
existing = col.get(where={"filename": basename}, include=["metadatas"])
if existing["ids"]:
    col.delete(ids=existing["ids"])
    old_count = len(existing["ids"])
else:
    old_count = 0

docs = [c["content"] for c in chunks]
ids = [f"{basename}:{i}" for i in range(len(chunks))]
metas = [{
    "source_file": filepath,
    "chunk_index": str(i),
    "wing": wing,
    "room": room,
    "filename": basename,
} for i in range(len(chunks))]

col.add(documents=docs, ids=ids, metadatas=metas)
if old_count:
    print(f"[palace-index] {basename}: replaced {old_count} → {len(chunks)} chunks ({wing}:{room})")
else:
    print(f"[palace-index] {basename}: {len(chunks)} chunks → {wing}:{room}")
