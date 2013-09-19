package com.findwise.tools;

public enum SystemTimeProvider implements TimeProvider {
	INSTANCE;

	@Override
	public long getCurrentTime() {
		return System.currentTimeMillis();
	}
	
}
