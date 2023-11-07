import os

import redis.asyncio as redis

pool = None

if os.getenv('REDIS_URI'):
    pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URI'))


def get_redis():
    if pool is not None:
        return redis.Redis(connection_pool=pool)
    else:
        raise Exception("REDIS_URI environment variable not set")
