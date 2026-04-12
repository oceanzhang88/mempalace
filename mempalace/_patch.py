"""Redis Patch v1.0 — Monkey-patch chromadb with Redis adapter.

Import this module BEFORE any mempalace code to redirect all
chromadb.PersistentClient calls to the Redis vectorset adapter.

Usage:
    import mempalace._patch  # noqa: F401 — activates Redis backend
    from mempalace.mcp_server import main
    main()

Or via environment variable:
    MEMPALACE_BACKEND=redis python -m mempalace.mcp_server
"""

import os
import sys
import types

# Only patch if backend is redis (default) or not explicitly chromadb
_backend = os.environ.get("MEMPALACE_BACKEND", "redis").lower()

if _backend == "redis":
    from . import redis_adapter

    # Create a fake 'chromadb' module that redirects to redis_adapter
    fake_chromadb = types.ModuleType("chromadb")
    fake_chromadb.PersistentClient = redis_adapter.PersistentClient
    fake_chromadb.Client = redis_adapter.RedisClient

    sys.modules["chromadb"] = fake_chromadb
