package com.findwise.hydra.mongodb;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.Document;
import com.findwise.hydra.TailableIterator;

public class TailReaderThread extends Thread {
	private TailableIterator<MongoType> it;
	public long lastRead = Long.MAX_VALUE;
	public DatabaseDocument<MongoType> lastReadDoc = null;
	boolean hasError = false;

	int countFailed = 0;
	int countProcessed = 0;
	int countDiscarded = 0;

	int count = 0;

	public TailReaderThread(TailableIterator<MongoType> it) {
		this.it = it;
	}

	public void run() {
		try {
			while (it.hasNext()) {
				lastRead = System.currentTimeMillis();
				lastReadDoc = it.next();

				Document.Status s = lastReadDoc.getStatus();

				if(s== Document.Status.DISCARDED) {
					countDiscarded++;
				} else if (s == Document.Status.PROCESSED) {
					countProcessed++;
				} else if (s == Document.Status.FAILED) {
					countFailed++;
				}

				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			hasError = true;
		}
	}

	public void interrupt() {
		it.interrupt();
	}
}
