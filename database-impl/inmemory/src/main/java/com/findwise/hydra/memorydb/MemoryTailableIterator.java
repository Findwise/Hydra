package com.findwise.hydra.memorydb;

import java.util.concurrent.BlockingQueue;
import java.util.prefs.BackingStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.TailableIterator;

public class MemoryTailableIterator implements TailableIterator<MemoryType> {
	
	private static Logger logger = LoggerFactory.getLogger(MemoryTailableIterator.class);
	private BlockingQueue<MemoryDocument> queue;
	
	private MemoryDocument peeked;
	private boolean[] modified;
	
	private boolean closed = false;
	
	/**
	 * @param dbc
	 * @throws BackingStoreException if there is a problem in getting an iterator
	 */
	public MemoryTailableIterator(BlockingQueue<MemoryDocument> queue, boolean[] modified) {
		this.queue = queue;
		this.modified = modified;
	}
	
	@Override
	public boolean hasNext() {
		if(modified[0]) {
			modified[0] = false;
			peeked = null;
		}
		
		if(peeked!=null) {
			return true;
		}
		
		while (!closed) {
			try {
				peeked = queue.take();
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.info("Interrupt caught during sleep", e);
				Thread.currentThread().interrupt();
			}
		}
		return false;
	}

	@Override
	public MemoryDocument next() {
		if(closed) {
			return null;
		}
		MemoryDocument d = peeked;
		peeked = null;
		return d;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() is not supported");
	}

	@Override
	public void interrupt() {
		closed = true;
	}

}
