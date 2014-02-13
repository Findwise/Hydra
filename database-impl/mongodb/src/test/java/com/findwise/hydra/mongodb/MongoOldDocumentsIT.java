package com.findwise.hydra.mongodb;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.TailableIterator;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoOldDocumentsIT {

	private final int discardedToKeep = 10;

	@Rule
	public MongoConnectorResource mongoConnectorResource = new MongoConnectorResource(getClass(),
			new MongoConfigurationBuilder().setOldMaxCount(discardedToKeep).build());

	@Test
	public void testRollover() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		assertTrue("NumberToKeep not correctly set", mdc.getStatusReader().getStatus().getNumberToKeep() == discardedToKeep);

		final int additionalDocuments = 1;
		for (int i = 0; i <= discardedToKeep + additionalDocuments; i++) {
			dw.insert(new MongoDocument());
			DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
			dw.markProcessed(dd, "tag");
		}

		if (mdc.getDocumentReader().getActiveDatabaseSize() != 0 ) {
			fail("Still some active docs..");
		}

		if (mdc.getDocumentReader().getInactiveDatabaseSize() != discardedToKeep) {
			fail("Incorrect number of old documents kept");
		}

		dw.insert(new MongoDocument());
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");

		if (mdc.getDocumentReader().getActiveDatabaseSize() != 0) {
			fail("Still some active docs..");
		}
		if (mdc.getDocumentReader().getInactiveDatabaseSize() != discardedToKeep) {
			fail("Incorrect number of old documents kept: "+ mdc.getDocumentReader().getInactiveDatabaseSize());
		}
	}

	@Test
	public void testInactiveIterator() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();

		TailableIterator<MongoType> it = mdc.getDocumentReader().getInactiveIterator();

		TailReaderThread tr = new TailReaderThread(it);
		tr.start();

		MongoDocument first = new MongoDocument();
		first.putContentField("num", 1);
		dw.insert(first);
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");

		while(tr.lastRead>System.currentTimeMillis() && tr.isAlive()) {
			Thread.sleep(50);
		}

		if(!tr.isAlive()) {
			fail("TailableReader died");
		}

		long lastRead = tr.lastRead;

		if(!tr.lastReadDoc.getContentField("num").equals(1)) {
			fail("Last doc read was not the correct document!");
		}

		MongoDocument second = new MongoDocument();
		second.putContentField("num", 2);
		dw.insert(second);
		dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");

		while(tr.lastRead==lastRead) {
			Thread.sleep(50);
		}

		if (!tr.lastReadDoc.getContentField("num").equals(2)) {
			fail("Last doc read was not the correct document!");
		}


		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator prior to interrupt");
		}

		tr.interrupt();

		long interrupt = System.currentTimeMillis();

		while (tr.isAlive() && (System.currentTimeMillis()-interrupt)<10000) {
			Thread.sleep(50);
		}

		if(tr.isAlive()) {
			fail("Unable to interrupt the tailableiterator");
		}

		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator after interrupt");
		}
	}
}
