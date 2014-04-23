package com.findwise.hydra.mongodb;

import java.util.prefs.BackingStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.TailableIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException.CursorNotFound;

public class MongoTailableIterator implements TailableIterator<MongoType> {
	
	private static Logger logger = LoggerFactory.getLogger(MongoTailableIterator.class);
	private DBCursor cursor;
	private DBCollection dbc;
	
	
	boolean closed = false;
	private DBObject query;

	
	
	/**
	 * @param dbc
	 * @throws BackingStoreException if there is a problem in getting an iterator
	 */
	public MongoTailableIterator(DBCollection dbc) throws BackingStoreException {	
		this(new BasicDBObject(), dbc);
	}
	
	public MongoTailableIterator(DBObject query, DBCollection dbc) throws BackingStoreException{
		this.dbc = dbc;
		this.query = query;
		createCursor();
	}
	
	private void createCursor() throws BackingStoreException {
		if(!dbc.isCapped()) {
			throw new BackingStoreException("Unable to create cursor: collection is not capped");
		}
		cursor = dbc.find(query)
				.sort(new BasicDBObject("$natural", 1))
				.addOption(Bytes.QUERYOPTION_TAILABLE)
				.addOption(Bytes.QUERYOPTION_AWAITDATA)
				.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
	}
	
	@Override
	public boolean hasNext() {
		ensureValidCursor();
		if (closed) {
			return false;
		}
		try {
			return cursor.hasNext();
		} catch(CursorNotFound e) {
			if(!closed) {
				logger.error("Cursor lost without iterator being closed", e);
			}
			return false;
		}
	}

	/**
	 * A freshly initialized capped collection will cause
	 * the cursor to return false on the first call to hasNext().
	 * This method ensures that the cursor waits for the first
	 * document to arrive.
	 */
	private void ensureValidCursor() {
		while (cursor.count() == 0 && !closed) {
			try {
				createCursor();
				Thread.sleep(500);
			} catch (BackingStoreException e) {
				logger.error("Unable to create cursor during call to hasNext()", e);
			} catch (InterruptedException e) {
				logger.info("Interrupt caught during sleep", e);
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public MongoDocument next() {
		if(closed) {
			return null;
		}
		return (MongoDocument) cursor.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() is not supported");
	}

	@Override
	public void interrupt() {
		closed = true;
		cursor.close();
	}

}
