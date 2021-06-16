package com.finsol.Redis;

public interface RedisSessionFactory {
    RedisSession create(RedisConnectorConfig config);
}