package com.findwise.hydra.mongodb;

import java.util.Iterator;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.TailableIterator;
import com.mongodb.DBCursor;
import com.mongodb.MongoException.CursorNotFound;

public class MongoTailableIterator implements TailableIterator<MongoType> {

	private Iterator<DatabaseDocument<MongoType>> iterator; 
	private DBCursor cursor;
	
	boolean closed = false;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MongoTailableIterator(DBCursor cursor) {
		this.cursor = cursor;
		this.iterator = (Iterator) cursor;
	}
	
	@Override
	public boolean hasNext() {
		try {
			if(closed) {
				return false;
			}
			return iterator.hasNext();
		} catch(CursorNotFound e) {
			return false;
		}
	}

	@Override
	public DatabaseDocument<MongoType> next() {
		if(closed) {
			return null;
		}
		return iterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() is not supported");
	}

	@Override
	public void interrupt() {
		cursor.close();
	}

}
