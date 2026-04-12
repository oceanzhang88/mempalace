#!/usr/bin/env python3
"""Launch MemPalace MCP server with Redis backend.

Usage:
    claude mcp add mempalace -- python Components/mempalace/mcp_redis.py
"""
import sys
import os

# Ensure mempalace is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Activate Redis backend BEFORE any mempalace imports
os.environ["MEMPALACE_BACKEND"] = "redis"
import mempalace._patch  # noqa: F401, E402

# Now run the MCP server
from mempalace.mcp_server import main  # noqa: E402
main()
