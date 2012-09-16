package com.findwise.hydra;


public class StatusUpdater extends Thread {
	private int processed;
	private int failed;
	private int discarded;
	
	private int interval;
	
	private DatabaseConnector<?> connector;
	
	/**
	 * Sets up a StatusUpdater with default update interval of 1000.
	 * 
	 * Equivalent of calling StatusUpdater(connector, 1000)
	 */
	public StatusUpdater(DatabaseConnector<?> connector) {
		this(connector, 1000);
	}
	
	public StatusUpdater(DatabaseConnector<?> connector, int updateIntervalMs) {
		this.connector = connector;
		this.interval = updateIntervalMs;
	}
	
	public void setUpdateInterval(int updateIntervalMs) {
		interval = updateIntervalMs;
	}
	
	public int getUpdateInterval() {
		return interval;
	}
	
	public synchronized void addProcessed(int toAdd) {
		processed += toAdd;
	}
	
	private synchronized int getAndClearProcessed() {
		int a = processed;
		processed = 0;
		return a;
	}
	
	public synchronized void addFailed(int toAdd) {
		failed += toAdd;
	}
	
	private synchronized int getAndClearFailed() {
		int a = failed;
		failed = 0;
		return a;
	}
	
	public synchronized void addDiscarded(int toAdd) {
		discarded += toAdd;
	}
	
	private synchronized int getAndClearDiscarded() {
		int a = discarded;
		discarded = 0;
		return a;
	}

	public void run() {
		while (!isInterrupted()) {
			try {
				saveStatus();
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				interrupt();
			}
		}
		saveStatus();
	}

	public void saveStatus() {
		connector.getStatusWriter().increment(getAndClearProcessed(), getAndClearFailed(), getAndClearDiscarded());
	}
}
