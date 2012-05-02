package com.findwise.hydra.common;

public interface JsonDeserializer {
	void fromJson(String json) throws JsonException;
}
