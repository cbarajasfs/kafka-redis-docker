package com.finsol.Redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public interface RedisSession extends AutoCloseable {
    AbstractRedisClient client();

    StatefulConnection connection();

    RedisClusterAsyncCommands<byte[], byte[]> asyncCommands();
}

