"""Redis Vectorset Adapter v1.0 — Drop-in replacement for ChromaDB.

Implements the ChromaDB PersistentClient + Collection interface using
Redis 8.0 native VADD/VSIM vectorset commands. Zero changes needed
to the rest of the MemPalace codebase — this module provides the same
API that ChromaDB exposes.

Usage:
    # Instead of: import chromadb
    # Use:        from .redis_adapter import RedisClient
    # Then:       client = RedisClient(path=palace_path)
    #             col = client.get_or_create_collection("mempalace_drawers")
    #             col.add(documents=[...], ids=[...], metadatas=[...])

Activated via _patch.py which replaces 'chromadb' in sys.modules.
"""

import hashlib
import json
import random
import struct
import time
from typing import Any

import redis

# Lazy-load sentence-transformers (heavy import)
_model = None
_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
_EMBEDDING_DIM = 384


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def _embed_texts(texts: list[str]) -> list[list[float]]:
    model = _get_model()
    return model.encode(texts, normalize_embeddings=True, batch_size=32).tolist()


def _vec_to_fp32(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


class RedisCollection:
    """Drop-in replacement for chromadb.Collection backed by Redis vectorset."""

    def __init__(self, r: redis.Redis, r_str: redis.Redis, name: str):
        self.r = r
        self.r_str = r_str
        self.name = name
        self._vset_key = f"mp:{name}:vectors"
        self._meta_prefix = f"mp:{name}:meta:"
        self._doc_prefix = f"mp:{name}:doc:"
        self._ids_key = f"mp:{name}:ids"

    def add(self, documents: list[str] = None, ids: list[str] = None,
            metadatas: list[dict] = None, embeddings: list = None, **kwargs):
        """Add documents to the collection."""
        if not ids:
            return
        if documents is None:
            documents = [""] * len(ids)
        if metadatas is None:
            metadatas = [{}] * len(ids)

        # Generate embeddings if not provided
        if embeddings is None:
            embeddings = _embed_texts(documents)

        for doc_id, doc, meta, emb in zip(ids, documents, metadatas, embeddings):
            fp32 = _vec_to_fp32(emb)

            # Store vector
            try:
                self.r.execute_command(
                    "VADD", self._vset_key, "REDUCE", _EMBEDDING_DIM,
                    "FP32", fp32, doc_id
                )
            except Exception as e:
                if "already exists" in str(e).lower():
                    # Update: remove and re-add
                    try:
                        self.r.execute_command("VREM", self._vset_key, doc_id)
                    except Exception:
                        pass
                    self.r.execute_command(
                        "VADD", self._vset_key, "REDUCE", _EMBEDDING_DIM,
                        "FP32", fp32, doc_id
                    )
                else:
                    raise

            # Store document text
            self.r_str.set(f"{self._doc_prefix}{doc_id}", doc)

            # Store metadata as JSON
            self.r_str.set(f"{self._meta_prefix}{doc_id}", json.dumps(meta, ensure_ascii=False))

            # Track ID in set
            self.r_str.sadd(self._ids_key, doc_id)

    def upsert(self, documents: list[str] = None, ids: list[str] = None,
               metadatas: list[dict] = None, embeddings: list = None, **kwargs):
        """Upsert = delete existing + add."""
        if ids:
            for doc_id in ids:
                try:
                    self.r.execute_command("VREM", self._vset_key, doc_id)
                except Exception:
                    pass
        self.add(documents=documents, ids=ids, metadatas=metadatas, embeddings=embeddings)

    def query(self, query_texts: list[str] = None, query_embeddings: list = None,
              n_results: int = 5, where: dict = None, include: list = None, **kwargs) -> dict:
        """Semantic search. Returns ChromaDB-compatible result dict."""
        if include is None:
            include = ["documents", "metadatas", "distances"]

        # Get query embedding
        if query_embeddings:
            q_emb = query_embeddings[0]
        elif query_texts:
            q_emb = _embed_texts(query_texts[:1])[0]
        else:
            return {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}

        fp32 = _vec_to_fp32(q_emb)

        try:
            raw = self.r.execute_command(
                "VSIM", self._vset_key, "FP32", fp32,
                "WITHSCORES", "COUNT", min(n_results * 3, 100)  # Overfetch for filtering
            )
        except Exception:
            return {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}

        result_ids = []
        result_docs = []
        result_metas = []
        result_distances = []

        for i in range(0, len(raw), 2):
            if len(result_ids) >= n_results:
                break

            eid = raw[i].decode() if isinstance(raw[i], bytes) else raw[i]
            score = float(raw[i + 1])

            # Load metadata
            meta_json = self.r_str.get(f"{self._meta_prefix}{eid}")
            meta = json.loads(meta_json) if meta_json else {}

            # Apply where filter if specified
            if where and not self._matches_where(meta, where):
                continue

            result_ids.append(eid)
            result_distances.append(1.0 - score)  # ChromaDB uses distance, VSIM uses similarity

            if "documents" in include:
                doc = self.r_str.get(f"{self._doc_prefix}{eid}") or ""
                result_docs.append(doc)

            if "metadatas" in include:
                result_metas.append(meta)

        result = {"ids": [result_ids], "distances": [result_distances]}
        if "documents" in include:
            result["documents"] = [result_docs]
        if "metadatas" in include:
            result["metadatas"] = [result_metas]

        return result

    def get(self, ids: list[str] = None, where: dict = None,
            include: list = None, limit: int = None, offset: int = 0, **kwargs) -> dict:
        """Get documents by ID or filter."""
        if include is None:
            include = ["documents", "metadatas"]

        if ids:
            all_ids = ids
        else:
            all_ids = sorted(self.r_str.smembers(self._ids_key))

        # Apply offset and limit
        if offset:
            all_ids = all_ids[offset:]
        if limit:
            all_ids = all_ids[:limit]

        result_ids = []
        result_docs = []
        result_metas = []

        for eid in all_ids:
            meta_json = self.r_str.get(f"{self._meta_prefix}{eid}")
            meta = json.loads(meta_json) if meta_json else {}

            if where and not self._matches_where(meta, where):
                continue

            result_ids.append(eid)

            if "documents" in include:
                doc = self.r_str.get(f"{self._doc_prefix}{eid}") or ""
                result_docs.append(doc)

            if "metadatas" in include:
                result_metas.append(meta)

        result = {"ids": result_ids}
        if "documents" in include:
            result["documents"] = result_docs
        if "metadatas" in include:
            result["metadatas"] = result_metas

        return result

    def delete(self, ids: list[str] = None, where: dict = None, **kwargs):
        """Delete documents by ID."""
        if ids is None:
            return

        for doc_id in ids:
            try:
                self.r.execute_command("VREM", self._vset_key, doc_id)
            except Exception:
                pass
            self.r_str.delete(f"{self._doc_prefix}{doc_id}")
            self.r_str.delete(f"{self._meta_prefix}{doc_id}")
            self.r_str.srem(self._ids_key, doc_id)

    def count(self) -> int:
        """Count total documents."""
        try:
            c = self.r.execute_command("VCARD", self._vset_key)
            return int(c) if c else 0
        except Exception:
            return int(self.r_str.scard(self._ids_key))

    def peek(self, limit: int = 10) -> dict:
        """Peek at first N documents."""
        return self.get(limit=limit, include=["documents", "metadatas"])

    def get_random_entries(self, room: str, count: int, wing: str = None) -> list[dict]:
        """Get N random drawers from a room. Returns list of {text, metadata}."""
        where = {"room": room}
        if wing:
            where = {"$and": [{"wing": wing}, {"room": room}]}
        result = self.get(where=where, include=["documents", "metadatas"])
        ids = result.get("ids", [])
        docs = result.get("documents", [])
        metas = result.get("metadatas", [])
        if not ids:
            return []
        indices = random.sample(range(len(ids)), min(count, len(ids)))
        return [{"text": docs[i], "metadata": metas[i]} for i in indices]

    def _matches_where(self, meta: dict, where: dict) -> bool:
        """Simple where clause matching (ChromaDB style)."""
        for key, val in where.items():
            if key.startswith("$"):
                # Logical operators ($and, $or)
                if key == "$and":
                    return all(self._matches_where(meta, clause) for clause in val)
                elif key == "$or":
                    return any(self._matches_where(meta, clause) for clause in val)
                continue

            meta_val = meta.get(key)
            if isinstance(val, dict):
                for op, operand in val.items():
                    if op == "$eq" and meta_val != operand:
                        return False
                    elif op == "$ne" and meta_val == operand:
                        return False
                    elif op == "$in" and meta_val not in operand:
                        return False
            else:
                if meta_val != val:
                    return False
        return True


class RedisClient:
    """Drop-in replacement for chromadb.PersistentClient."""

    def __init__(self, path: str = None, host: str = "localhost",
                 port: int = 6379, db: int = 0, **kwargs):
        self.path = path  # Kept for API compat, not used for Redis
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=False)
        self.r_str = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._collections = {}

    def get_or_create_collection(self, name: str, **kwargs) -> RedisCollection:
        if name not in self._collections:
            self._collections[name] = RedisCollection(self.r, self.r_str, name)
        return self._collections[name]

    def get_collection(self, name: str, **kwargs) -> RedisCollection:
        return self.get_or_create_collection(name)

    def list_collections(self) -> list:
        return list(self._collections.values())


# Module-level factory for API compat
def PersistentClient(path: str = None, **kwargs) -> RedisClient:
    """Drop-in for chromadb.PersistentClient(path=...)"""
    return RedisClient(path=path, **kwargs)
