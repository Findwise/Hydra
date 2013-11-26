package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapConfiguration implements Configuration {
	protected Map<String, String> map;

	public MapConfiguration() {
		map = new HashMap<String, String>();
	}

	@Override
	public String getParameter(String key) {
		if (map.containsKey(key)) {
			return map.get(key);
		}
		throw new NoSuchElementException("No setting found for key: " + key);
	}

	public void setParameter(String key, String value) {
		map.put(key, value);
	}

	@Override
	public String getParameter(String key, String defaultValue) {
		if (map.containsKey(key)) {
			return map.get(key);
		}
		return defaultValue;
	}
}
