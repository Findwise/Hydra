package com.findwise.hydra;

public interface JsonDeserializer {
	void fromJson(String json) throws JsonException;
}
