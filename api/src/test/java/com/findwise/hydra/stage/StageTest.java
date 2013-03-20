package com.findwise.hydra.stage;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.fail;

import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

public class StageTest {
	
	@Test	
	public void testProcessParameterInjection() throws Exception {
		ProcessStage ps = new ProcessStage();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("privateString", "1");
		map.put("protectedString", "2");
		List<String> list = new ArrayList<String>();
		list.add("3");
		list.add("4");
		map.put("stringList", list);
		ps.setParameters(map);
		
		if(!"1".equals(ps.privateString)) {
			fail("privateString not correctly set");
		}
		
		if(!"2".equals(ps.protectedString)) {
			fail("protectedString not correctly set");
		}
		
		if(ps.stringList == null) {
			fail("stringList is null");
		}
		
		for(int i=0; i<list.size(); i++) {
			if(!ps.stringList.get(i).equals(list.get(i))) {
				fail("Mismatch between supplied and found lists in element "+i);
			}
		}
	}
	
	@Test
	public void testInhertiedParameterInjection() throws Exception {
		ExtendedProcessStage ps = new ExtendedProcessStage();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("privateString", "1");
		map.put("protectedString", "2");
		List<String> list = new ArrayList<String>();
		list.add("3");
		list.add("4");
		map.put("stringList", list);
		ps.setParameters(map);
		
		if(!"1".equals(ps.getPrivateString())) {
			fail("privateString not correctly set");
		}
		
		if(!"2".equals(ps.protectedString)) {
			fail("protectedString not correctly set");
		}
		
		if(ps.stringList == null) {
			fail("stringList is null");
		}
		
		for(int i=0; i<list.size(); i++) {
			if(!ps.stringList.get(i).equals(list.get(i))) {
				fail("Mismatch between supplied and found lists in element "+i);
			}
		}
	}
	
	@Test 
	public void testJsonDeserializerParameterInjection() throws Exception {
		ProcessStage ps = new ProcessStage();
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		LocalQuery query = new LocalQuery();
		query.requireAction(Action.ADD);
		query.requireContentFieldEquals("equals", 1);
		query.requireContentFieldExists("exists");
		query.requireContentFieldNotExists("notexists");
		map.put("query", SerializationUtils.fromJson(query.toJson()));
		ps.setParameters(map);
		
		if(ps.getQuery().getAction()!=Action.ADD) {
			fail("Did not get correct action");
		}

		if(!ps.getQuery().getContentsEquals().keySet().contains("equals")) {
			fail("Did not have an equals field");
		}
		
		if(!ps.getQuery().getContentsEquals().get("equals").equals(1)) {
			fail("Did not have correct equals value");
		}
		
		if(!ps.getQuery().getContentsExists().get("exists")) {
			fail("Did not have correct exists field");
		}
		
		if(ps.getQuery().getContentsExists().get("notexists")) {
			fail("Did not have correct notexists field");
		}
		
	}

	@Stage
	public static class ProcessStage extends AbstractProcessStage {
		@Parameter
		private String privateString;
		
		@Parameter
		protected String protectedString;
		
		@Parameter
		public List<String> stringList;
		
		@Override
		public void process(LocalDocument doc) throws ProcessException {
		}

		@Override
		public void init() throws RequiredArgumentMissingException {
		}
		
		public String getPrivateString() {
			return privateString;
		}
	}
	
	@Stage
	public static class ExtendedProcessStage extends ProcessStage {
		
	}
}
