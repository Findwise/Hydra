package com.findwise.hydra;

public interface ShutdownHandler {
	public boolean isShuttingDown();
	public void shutdown();
}
