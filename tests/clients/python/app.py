import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import redis.asyncio as redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await redis_client.ping()
    yield
    await redis_client.aclose()


app = FastAPI(title="Postgres-Redis Gateway API", lifespan=lifespan)


@app.post("/cache/{key}")
async def set_cache_value(key: str, value: str):
    await redis_client.set(key, value)
    return {"status": "success", "key": key}


@app.get("/cache/{key}")
async def get_cache_value(key: str):
    val = await redis_client.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": val}
