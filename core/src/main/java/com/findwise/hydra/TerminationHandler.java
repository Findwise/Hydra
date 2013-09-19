package com.findwise.hydra;

public interface TerminationHandler {
	public boolean isTerminating();
	public void shutdown();
}
