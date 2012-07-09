package com.findwise.hydra.memorydb;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Document.Action;

@RunWith(RepeatRunner.class)
public class MemoryDocumentTest {
	
	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	@Repeat(100)
	public void testBlank() {
		if(!TestTools.getRandomDocument().matches(new MemoryQuery())) {
			fail("Document did not match the empty query");
		}
	}
	
	@Test
	@Repeat(100)
	public void testContentExists() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();

		String field;
		do {
			field = TestTools.getRandomString(10);
		} while (md.hasContentField(field));

		MemoryQuery mq = new MemoryQuery();
		mq.requireContentFieldExists(field);
		if(md.matches(mq)) {
			fail("Matched for field that the document didn't have");
		}
		
		while (md.getContentFields().size()==0) {
			md = TestTools.getRandomDocument();
		}
		field = md.getContentFields().iterator().next(); 
		
		mq = new MemoryQuery();
		mq.requireContentFieldExists(field);
		
		if(!md.matches(mq)) {
			fail("Did not match field that should have matched");
		}
	}
	
	@Test
	@Repeat(100)
	public void testContentEquals() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();

		String field;
		do {
			field = TestTools.getRandomString(10);
		} while (md.hasContentField(field));

		MemoryQuery mq = new MemoryQuery();
		String value = TestTools.getRandomString(10);
		mq.requireContentFieldEquals(field, value);
		
		if(md.matches(mq)) {
			fail("Matched for field that the document didn't have");
		}
		
		md.putContentField(field, value);
		
		if(!md.matches(mq)) {
			fail("Didn't match for field that the document had");
		}
		
		md.putContentField(field, 1);
		mq = new MemoryQuery();
		mq.requireContentFieldEquals(field, 1);
		if(!md.matches(mq)) {
			fail("Didn't match for field with integer value");
		}
	}
	
	@Test
	public void testActionEquals() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();		
		
		MemoryQuery mq = new MemoryQuery();
		mq.requireAction(Action.DELETE);
		
		if(md.matches(mq)) {
			fail("Matched for an action that the document didn't have");
		}
		
		md.setAction(Action.DELETE);
		
		if(!md.matches(mq)) {
			fail("Didn't match for action that the document had");
		}
	}
	
	@Test
	public void testTouchedBy() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();
		
		String stageName;
		do {
			stageName = TestTools.getRandomString(10);
		} while(md.touchedBy(stageName));
		
		MemoryQuery mq = new MemoryQuery();
		mq.requireTouchedByStage(stageName);
		
		if(md.matches(mq)) {
			fail("Matched for a stage name that the document hasn't been touched by");
		}
		
		md.tag(Document.TOUCHED_METADATA_TAG, stageName);
		
		if(!md.matches(mq)) {
			fail("Didn't match for a stage name that the document has been touched by");
		}
	}
	
	@Test
	public void testNotTouchedBy() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();
		
		String stageName;
		do {
			stageName = TestTools.getRandomString(10);
		} while(md.touchedBy(stageName));
		
		MemoryQuery mq = new MemoryQuery();
		mq.requireNotTouchedByStage(stageName);
		
		if(!md.matches(mq)) {
			fail("Didn't match for a stage name that the document hasn't been touched by");
		}
		
		md.tag(Document.TOUCHED_METADATA_TAG, stageName);
		
		if(md.matches(mq)) {
			fail("Matched for a stage name that the document has been touched by");
		}
	}
	
	
}
