package com.findwise.hydra.mongodb;

import com.findwise.hydra.DatabaseDocument;

public class DocumentInserterThread extends Thread {

	private final int testReadCount;
	private final MongoConnector mdc;

	public DocumentInserterThread(final int testReadCount, final MongoConnector mongoConnector) {
		super();
		this.testReadCount = testReadCount;
		this.mdc = mongoConnector;
	}

	public void run() {
		try {
			insertDocuments(testReadCount);
			processDocuments(testReadCount/3);
			failDocuments(testReadCount/3);
			discardDocuments(testReadCount - (testReadCount/3)*2);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public long processDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markProcessed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}

	public long failDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markFailed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}

	public long discardDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markDiscarded(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}

	public long insertDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField("some_field_" + i, "a string with number " + i);
			mdc.getDocumentWriter().insert(d);
		}
		return System.currentTimeMillis()-start;
	}
}
