package com.finsol.redkaf.connector;

import java.util.HashMap;
import java.util.Map;

import io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector;

public class RedisConnector {

	public RedisSinkConnector redisSinkConnector;

	public RedisConnector() {

		try {
			Map<String, String> props = new HashMap<>();

			props.put("redis.uri", "redis://localhost:6379");
			props.put("redis.cluster.enabled", "false");

			redisSinkConnector.start(props);			
		} catch(Exception ex) {
			System.out.println(ex.getMessage());
		}

	}
	
	
}
