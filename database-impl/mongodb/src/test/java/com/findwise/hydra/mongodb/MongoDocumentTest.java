package com.findwise.hydra.mongodb;

import junit.framework.Assert;

import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class MongoDocumentTest {

	@Test
	public void testNullTransfer() throws Exception {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", "value");
		ld.markSynced();
		ld.removeContentField("field");
		
		MongoDocument md = new MongoDocument(ld.toJson());
		
		Assert.assertEquals(1, md.getTouchedContent().size());
		Assert.assertNull(md.getContentField("field"));
	}
}
