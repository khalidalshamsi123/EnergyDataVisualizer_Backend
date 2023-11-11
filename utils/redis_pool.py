import os

import redis.asyncio as redis

pool = None

if os.getenv('REDIS_URI', 'redis://localhost:6379'):
    pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URI', 'redis://localhost:6379'))


def get_redis():
    # if pool is not None:
        return redis.Redis(connection_pool=pool)
    # else:
    #     raise Exception("REDIS_URI environment variable not set")
