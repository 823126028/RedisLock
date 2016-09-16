package com.distribute.redis.lock;

import java.util.UUID;

public class RawUUIDGenerator implements UUIDGenerator {
	public String generate() {
		return UUID.randomUUID().toString();
	}
}
