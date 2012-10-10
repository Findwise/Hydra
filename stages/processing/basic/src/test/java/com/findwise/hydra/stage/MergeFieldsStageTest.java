package com.findwise.hydra.stage;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class MergeFieldsStageTest {
	
	@Test
	public void testInit() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		mfs.setParameters(map);
		
		try {
			mfs.init();
			Assert.fail("Did not throw RequiredArgumentMissing");
		} catch(RequiredArgumentMissingException e) {
		}
		
		map.put("outputField", "out");
		map.put("fromFields", new ArrayList<String>());
		
		mfs.setParameters(map);
		
		mfs.init();
	}
	
	@Test
	public void testProcessNoFromFields() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("fromFields", new ArrayList<String>());
		
		mfs.setParameters(map);
		
		LocalDocument doc = new LocalDocument();
		doc.putContentField("out", "xyz");
		doc.putContentField("test", "test");
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertTrue(doc.isEqual(doc2));
	}
	
	@Test
	public void testProcessFieldNames() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = getDocument();
		doc.putContentField("out", "xyz");
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertEquals(doc.getContentField("in1"), doc2.getContentField("in1"));
		Assert.assertEquals(doc.getContentField("in2"), doc2.getContentField("in2"));
		Assert.assertEquals(doc.getContentField("in3"), doc2.getContentField("in3"));
		
		String[] outString = doc2.getContentField("out").toString().split(" ");
		
		Assert.assertEquals(4, outString.length);
		for(String s : outString) {
			Assert.assertTrue(hasValue(doc, s));
		}
	}
	
	@Test
	public void testProcessMultipleMatches() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in1");
		list.add("in2");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = getDocument();
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		String[] outString = doc2.getContentField("out").toString().split(" ");
		
		Assert.assertEquals(2, outString.length);
		for(String s : outString) {
			Assert.assertTrue(hasValue(doc, s));
		}
	}
	
	@Test
	public void testSeparator() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("separator", "___");
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = getDocument();
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		String[] outString = doc2.getContentField("out").toString().split("___");
		Assert.assertEquals(3, outString.length);
		
		for(String s : outString) {
			Assert.assertTrue(hasValue(doc, s));
		}
		
		Assert.assertEquals("___", doc2.getContentField("out").toString().substring(3,6));
		Assert.assertEquals("___", doc2.getContentField("out").toString().substring(9,12));
	}
	
	@Test
	public void testClearOutputField() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("clearOutputField", true);

		LocalDocument doc = getDocument();
		doc.putContentField("out", "xyz");

		List<String> list =  new ArrayList<String>();
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertNull(doc2.getContentField("out"));
		
		list.add("in1");
		list.add("in2");
		list.add("in3");
		
		mfs.setParameters(map);
		
		doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		String[] outString = doc2.getContentField("out").toString().split(" ");
		
		Assert.assertEquals(3, outString.length);
		for(String s : outString) {
			Assert.assertTrue(hasValue(doc, s));
		}
	}
	
	@Test
	public void testProcessRegexNames() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		
		List<String> list =  new ArrayList<String>();
		list.add("in[0-9]*");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = getDocument();
		doc.putContentField("out", "xyz");
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		String[] outString = doc2.getContentField("out").toString().split(" ");
		
		Assert.assertEquals(4, outString.length);
		
		for(String s : outString) {
			Assert.assertTrue(hasValue(doc, s));
		}
	}
	
	@Test
	public void testProcessExtraFieldNames() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "newfield");
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = getDocument();
		doc.putContentField("out", "xyz");
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);

		String[] newString = doc2.getContentField("newfield").toString().split(" ");
		
		Assert.assertEquals(2, newString.length);
		for(String s : newString) {
			Assert.assertTrue(hasValue(doc, s));
		}
	}
	
	@Test
	public void testProcessIntegerFields() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("additionIfNumbers", true);
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = new LocalDocument();
		doc.putContentField("in1", 1);
		doc.putContentField("in2", 2);
		doc.putContentField("in3", 5);
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertEquals(1+2+5, doc2.getContentField("out"));
	}
	
	@Test
	public void testProcessDoubleFields() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("additionIfNumbers", true);
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = new LocalDocument();
		doc.putContentField("in1", 1);
		doc.putContentField("in2", 2.2);
		doc.putContentField("in3", 5.3);
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertEquals(8.5, doc2.getContentField("out"));
	}
	
	@Test
	public void testProcessMixedFields() throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("additionIfNumbers", true);
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc = new LocalDocument();
		doc.putContentField("in1", 1);
		doc.putContentField("in2", 2.2);
		doc.putContentField("in3", "string");
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		Assert.assertEquals(String.class, doc2.getContentField("out").getClass());
	}

	private LocalDocument getDocument() {
		LocalDocument doc = new LocalDocument();
		doc.putContentField("in1", "in1");
		doc.putContentField("in2", "in2");
		doc.putContentField("in3", "in3");
		return doc;
	}
	
	@Test
	public void testToList() throws Exception {
		LocalDocument doc2 = testList(getDocument());

		Assert.assertEquals(3, ((List<?>)doc2.getContentField("out")).size());
	}
	
	@Test
	public void testAddList() throws Exception {
		LocalDocument doc = getDocument();
		doc.putContentField("out", Arrays.asList("out", "out2"));
		
		LocalDocument doc2 = testList(doc);
		Assert.assertEquals(5, ((List<?>)doc2.getContentField("out")).size());
	}
	
	@Test
	public void testCreateList() throws Exception {
		LocalDocument doc = getDocument();
		doc.putContentField("out", "out");
		
		LocalDocument doc2 = testList(doc);
		
		Assert.assertTrue(((List<?>)doc2.getContentField("out")).contains("out"));
		Assert.assertEquals(4, ((List<?>)doc2.getContentField("out")).size());
	}
	
	public LocalDocument testList(LocalDocument doc) throws Exception {
		MergeFieldsStage mfs = new MergeFieldsStage();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("outputField", "out");
		map.put("createList", true);
		
		List<String> list =  new ArrayList<String>();
		list.add("in1");
		list.add("in2");
		list.add("in3");
		map.put("fromFields", list);
		
		mfs.setParameters(map);
		
		LocalDocument doc2 = new LocalDocument(doc.toJson());
		mfs.process(doc2);
		
		
		Assert.assertEquals(doc.getContentField("in1"), doc2.getContentField("in1"));
		Assert.assertEquals(doc.getContentField("in2"), doc2.getContentField("in2"));
		Assert.assertEquals(doc.getContentField("in3"), doc2.getContentField("in3"));
		
		Assert.assertTrue(doc2.getContentField("out") instanceof List);
		List<?> outList = (List<?>)doc2.getContentField("out");
		Assert.assertTrue(outList.contains(doc.getContentField("in1")));
		Assert.assertTrue(outList.contains(doc.getContentField("in2")));
		Assert.assertTrue(outList.contains(doc.getContentField("in3")));
		
		
		return doc2;
	}
	
	private boolean hasValue(LocalDocument doc, String s) {
		for(String f : doc.getContentFields()) {
			if(doc.getContentField(f).equals(s)) {
				return true;
			}
		}
		return false;
	}
	
}
