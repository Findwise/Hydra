package com.findwise.hydra.mongodb;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.Document.Action;
import com.findwise.tools.Comparator;

public class DocumentJSONTest {

	MongoDocument test;
	MongoDocument test2;
	
	@Before
	public void setUp() throws Exception {
		test = new MongoDocument();
		test.setAction(Action.ADD);
		test.putContentField("name", "test");
		test.putContentField("number", 1);
		long time = new Date().getTime();
		test.putMetadataField("date", time);
		
		test2 = new MongoDocument();
		test2.setAction(Action.ADD);
		test2.putContentField("name", "test");
		test2.putContentField("number", 2);
		test2.putMetadataField("date", time);
	}

	@Test
	public void testEquals() {
		if(test.isEqual(test2)) {
			fail("Two different documents (in content) are equal");
		}

		test2.putContentField("number", 1);
		if(!test.isEqual(test2)) {
			fail("Two equal documents are not equal");
		}
		
		test.putMetadataField("x", 1);
		if(test.isEqual(test2)) {
			fail("Two different documents (in metadata) are equal");
		}
		
		test.setAction(Action.DELETE);
		
		if(test.isEqual(test2)) {
			fail("Two different documents (in ACTIONS) are equal");
		}
		
		
	}
	
	@Test
	public void testJSON() throws Exception {
		String json = test.toJson();
		test.putContentField("number", 2);
		if(json.equals(test.toJson())) {
			fail("Changes to content are not propagated to JSON");
		}
		json = test.toJson();
		test.putMetadataField("x", 1);
		if(json.equals(test.toJson())) {
			fail("Changes to metadata are not propagated to JSON");
		}
		
		if(test.isEqual(test2)) {
			fail("test2 and test are equal"); //Just a sanity check on equals
		}
		
		MongoDocument test3 = new MongoDocument();
		test3.fromJson(test.toJson());
		if(!test.isEqual(test3)) {
			fail("JSON-generated document is not equal to the JSON source");
		}
		
		if(test3.getAction()!=test.getAction()) {
			fail("JSON-generated document didn't have the correct actino");
		}

		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFieldsToJSON() throws JsonException {
		List<String>s = Arrays.asList("blahonga");
		String json = test.contentFieldsToJson(s);
		
		if((SerializationUtils.fromJson(json)).size()!=2) {
			fail("Did not return correct number of fields. Expected '<contents key>' and '_id'");
		}
		
		s.set(0, "name");
		json = test.contentFieldsToJson(s);
		
		if((SerializationUtils.fromJson(json)).size()!=2) {
			fail("Did not return correct number of fields. Expected '<contents key>' and '_id'");
		}

		Map<String, Object> m = (Map<String, Object>) SerializationUtils.fromJson(json).get(Document.CONTENTS_KEY);
		if(!m.containsKey("name")) {
			fail("Did not get the field 'name' returned");
		}
		if(!m.get("name").equals("test")) {
			fail("Did not get correct content for field 'name'");
		}
		
		String[] s2 = {"name", "number", "blahonga"};
		json = test.contentFieldsToJson(Arrays.asList(s2));
		
		m = (Map<String, Object>) SerializationUtils.fromJson(json).get(Document.CONTENTS_KEY);
		if(m.size()!=3) {
			fail("Did not return correct number of fields. Expected 'test, 'number', 'blahonga'");
		}
		if(!m.containsKey("name")) {
			fail("Did not get the field 'name' returned");
		}
		if(!Comparator.equals(m.get("number"), 1)) {
			fail("Did not get correct content for field 'number'");
		}
	}
	
	
}
