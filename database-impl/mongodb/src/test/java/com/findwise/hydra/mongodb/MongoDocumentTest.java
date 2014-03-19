package com.findwise.hydra.mongodb;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;

public class MongoDocumentTest {

	@Test
	public void testNullTransfer() throws Exception {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", "value");
		ld.markSynced();
		ld.removeContentField("field");
		
		MongoDocument md = new MongoDocument(ld.toJson());
		
		assertEquals(1, md.getTouchedContent().size());
		assertNull(md.getContentField("field"));
	}
	
	@Test
	public void testMatchesAction() {
		MongoDocument md = new MongoDocument();
		md.setAction(Action.ADD);
		
		MongoQuery mq = new MongoQuery();
		mq.requireAction(Action.DELETE);
		
		assertFalse(md.matches(mq));
		
		mq.requireAction(Action.ADD);
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesID() {
		MongoDocument md = new MongoDocument();
		MongoDocumentID id = new MongoDocumentID(new ObjectId());
		md.setID(id);
		
		MongoQuery mq = new MongoQuery();
		MongoDocumentID id2 = new MongoDocumentID(new ObjectId());
		
		if(id.equals(id2)) {
			fail("Id's are the same. Test failure.");
		}
		
		mq.requireID(id2);
		assertFalse(md.matches(mq));
		
		mq.requireID(id);
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesContentExists() {
		MongoDocument md = new MongoDocument();
		md.putContentField("string", "x");
		md.putContentField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireContentFieldExists("double");
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldExists("int");
		mq.requireContentFieldExists("string");
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesContentNotExists() {
		MongoDocument md = new MongoDocument();
		md.putContentField("string", "x");
		md.putContentField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireContentFieldNotExists("double");
		
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldNotExists("int");
		mq.requireContentFieldNotExists("string");
		assertFalse(md.matches(mq));
	}
	
	@Test
	public void testMatchesMetadataExists() {
		MongoDocument md = new MongoDocument();
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireMetadataFieldExists("double");
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireMetadataFieldExists("int");
		mq.requireMetadataFieldExists("string");
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesMetadataNotExists() {
		MongoDocument md = new MongoDocument();
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireMetadataFieldNotExists("double");
		
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireMetadataFieldNotExists("int");
		mq.requireMetadataFieldNotExists("string");
		assertFalse(md.matches(mq));
	}
	

	@Test
	public void testMatchesContentEquals() {
		MongoDocument md = new MongoDocument();
		md.putContentField("string", "x");
		md.putContentField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireContentFieldEquals("string", "y");
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldEquals("string", "x");
		mq.requireContentFieldEquals("int", 1);
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldEquals("double", 1.1);
		assertFalse(md.matches(mq));
	}
	

	@Test
	public void testMatchesContentNotEquals() {
		MongoDocument md = new MongoDocument();
		md.putContentField("string", "x");
		md.putContentField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireContentFieldNotEquals("string", "y");
		
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldNotEquals("string", "x");
		mq.requireContentFieldNotEquals("int", 1);
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldNotEquals("double", 1.1);
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesMetadataEquals() {
		MongoDocument md = new MongoDocument();
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireMetadataFieldEquals("string", "y");
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireMetadataFieldEquals("string", "x");
		mq.requireMetadataFieldEquals("int", 1);
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireContentFieldEquals("double", 1.1);
		assertFalse(md.matches(mq));
	}
	

	@Test
	public void testMatchesMetadataNotEquals() {
		MongoDocument md = new MongoDocument();
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		
		MongoQuery mq = new MongoQuery();
		mq.requireMetadataFieldNotEquals("string", "y");
		
		assertTrue(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireMetadataFieldNotEquals("string", "x");
		mq.requireMetadataFieldNotEquals("int", 1);
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireMetadataFieldNotEquals("double", 1.1);
		assertTrue(md.matches(mq));
	}
	
	@Test
	public void testMatchesFetchedBy() {
		MongoDocument md = new MongoDocument();
		
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		md.setFetchedBy("stage", new Date());
		
		MongoQuery mq = new MongoQuery();
		mq.requireFetchedByStage("stage");
		
		assertTrue(md.matches(mq));
		
		mq.requireFetchedByStage("stage2");
		assertFalse(md.matches(mq));
	}
	
	@Test
	public void testMatchesNotFetchedBy() {
		MongoDocument md = new MongoDocument();
		
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		md.setFetchedBy("stage", new Date());
		
		MongoQuery mq = new MongoQuery();
		mq.requireNotFetchedByStage("stage");
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireNotFetchedByStage("stage2");
		assertTrue(md.matches(mq));
	}

	@Test
	public void testRemoveFetchedBy() {
		MongoDocument md = new MongoDocument();
		
		md.setFetchedBy("stage", new Date());
		md.setFetchedBy("stage2", new Date());
		assertEquals(2, md.getFetchedBy().size());
		assertEquals(Arrays.asList("stage", "stage2"),
				md.getMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST));

		md.removeFetchedBy("stage2");
		assertEquals(1, md.getFetchedBy().size());
		assertTrue(md.fetchedBy("stage"));
		assertEquals(Arrays.asList("stage"),
				md.getMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST));
	}
	
	@Test
	public void testMatchesTouchedBy() {
		MongoDocument md = new MongoDocument();
		
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		md.setTouchedBy("stage", new Date());
		
		MongoQuery mq = new MongoQuery();
		mq.requireTouchedByStage("stage");
		
		assertTrue(md.matches(mq));
		
		mq.requireTouchedByStage("stage2");
		assertFalse(md.matches(mq));
	}
	
	@Test
	public void testMatchesNotTouchedBy() {
		MongoDocument md = new MongoDocument();
		
		md.putMetadataField("string", "x");
		md.putMetadataField("int", 1);
		md.setTouchedBy("stage", new Date());
		
		MongoQuery mq = new MongoQuery();
		mq.requireNotTouchedByStage("stage");
		
		
		assertFalse(md.matches(mq));
		
		mq = new MongoQuery();
		mq.requireNotTouchedByStage("stage2");
		assertTrue(md.matches(mq));
	}

	@Test
	public void testPutMetadataField_adds_to_fetchedList() {
		MongoDocument md = new MongoDocument();
		Map<String, Date> fetched = new HashMap<String, Date>();
		fetched.put("somestage", new Date());
		md.putMetadataField(Document.FETCHED_METADATA_TAG, fetched);

		assertTrue(md.hasMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST));
		assertEquals(Arrays.asList("somestage"), md.getMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST));
	}

	@Test
	public void testPutMetadataField_does_not_append_to_fetchedList_when_stage_already_there() {
		MongoDocument md = new MongoDocument();
		Map<String, Date> fetched1 = new HashMap<String, Date>();
		fetched1.put("somestage", new Date());
		md.putMetadataField(Document.FETCHED_METADATA_TAG, fetched1);
		Map<String, Date> fetched2 = new HashMap<String, Date>();
		fetched2.put("somestage", new Date());
		fetched2.put("someOtherStage", new Date());
		md.putMetadataField(Document.FETCHED_METADATA_TAG, fetched2);

		assertTrue(md.hasMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST));
		List<String> expected = Arrays.asList("somestage", "someOtherStage");
		List<?> actual = (List<?>) md.getMetadataField(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST);
		assertEquals(expected.size(), actual.size());
		assertTrue(actual.containsAll(expected));
		assertFalse(Collections.disjoint(expected, actual));
	}

}
