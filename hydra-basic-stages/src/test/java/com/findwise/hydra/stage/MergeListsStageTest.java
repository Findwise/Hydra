package com.findwise.hydra.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import junit.framework.Assert;

public class MergeListsStageTest {

	private MergeListsStage stage;
	private LocalDocument doc;

	@Before
	public void setUp() {
		stage = new MergeListsStage();
		doc = new LocalDocument();
	}	
	
	@Test
	public void testSingleInFieldIsUnaffected() throws ProcessException {
		List<String> inFields = new ArrayList<String>();
		inFields.add("list");
		stage.inFields = inFields;
		stage.outField = "out";
		stage.separator = ";";
	
		List<String> list1 = new ArrayList<String>();
		list1.add("a");
		list1.add("b");
		doc.putContentField("list", list1);
		
		stage.process(doc);
		
		String correctList = "[a, b]";
		String resultList = doc.getContentField(stage.outField).toString();
		assertTrue("Got: " + resultList + " expected " + correctList,
					resultList.equals(correctList));
	}
	
	@Test
	public void testCanMergeTwoLists() throws ProcessException {
		List<String> inFields = new ArrayList<String>();
		inFields.add("list1");
		inFields.add("list2");
		stage.inFields = inFields;
		stage.outField = "out";
		stage.separator = ";";
	
		List<String> list1 = new ArrayList<String>();
		list1.add("a");
		list1.add("b");
		doc.putContentField("list1", list1);
		List<String> list2 = new ArrayList<String>();
		list2.add("1");
		list2.add("2");
		doc.putContentField("list2", list2);
		
		stage.process(doc);
		
		String correctList = "[a;1, b;2]";
		String resultList = doc.getContentField(stage.outField).toString();
		assertTrue("Got: " + resultList + " expected " + correctList,
					resultList.equals(correctList));
	}
	
	
	
	@Test
	public void testSkipDifferentLengthLists() throws ProcessException {
		List<String> inFields = new ArrayList<String>();
		inFields.add("list1");
		inFields.add("list2");
		inFields.add("list3");
		stage.inFields = inFields;
		stage.outField = "out";
		stage.separator = ";";
	
		List<String> list1 = new ArrayList<String>();
		list1.add("a");
		list1.add("b");
		list1.add("c");
		doc.putContentField("list1", list1);
		List<String> list2 = new ArrayList<String>();
		list2.add("1");
		list2.add("2");
		list2.add("3");
		doc.putContentField("list2", list2);
		List<String> list3 = new ArrayList<String>();
		list2.add("A");
		list2.add("B");
		doc.putContentField("list3", list3);
		
		stage.process(doc);
		
		assertTrue("Expected outField " + stage.outField + " to be null",
					doc.getContentField(stage.outField) == null);
	}
	
	@Test
	public void testSkipNonLists() throws ProcessException {
		List<String> inFields = new ArrayList<String>();
		inFields.add("list");
		inFields.add("notalist");
		stage.inFields = inFields;
		stage.outField = "out";
		stage.separator = ";";
	
		List<String> list1 = new ArrayList<String>();
		list1.add("a");
		list1.add("b");
		doc.putContentField("list", list1);
		Object notalist = new Object();
		doc.putContentField("notalist", notalist);
		
		stage.process(doc);

		assertTrue("Expected outField " + stage.outField + " to be null",
					doc.getContentField(stage.outField) == null);
	}
	
	@Test
	public void testSkipNonexistingFields() throws ProcessException {
		List<String> inFields = new ArrayList<String>();
		inFields.add("nonexistingfield");
		stage.inFields = inFields;
		stage.outField = "out";
		stage.separator = ";";
		
		stage.process(doc);

		assertTrue("Expected outField " + stage.outField + " to be null",
					doc.getContentField(stage.outField) == null);
	}
}
