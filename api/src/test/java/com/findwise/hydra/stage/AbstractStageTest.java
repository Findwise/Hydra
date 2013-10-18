package com.findwise.hydra.stage;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.JsonException;
import org.junit.Test;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.stage.AbstractStageTest.TestStage.Enumerable;

public class AbstractStageTest {

	@Test
	public void testSetParameters_sets_all_supplied_parameters() throws Exception {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("string", "string");
		parameters.put("integer", 100);
		parameters.put("e1", "A");
		parameters.put("e2", 1);
		parameters.put("externalName", "a string");
		parameters.put("requiredInteger", 2);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("key", "value");
		parameters.put("map", map);
		List<String> list = new ArrayList<String>();
		list.add("item");
		parameters.put("list", list);
		LocalQuery query = new LocalQuery();
		query.requireAction(Action.ADD);
		parameters.put("query", query);
		
		Map<String, Object> serialized = SerializationUtils.fromJson(SerializationUtils.toJson(parameters));
		
		TestStage stage = new TestStage();
		stage.setParameters(serialized);
		
		assertEquals("string", stage.string);
		assertEquals(100, stage.integer);
		assertEquals(Enumerable.A, stage.e1);
		assertEquals(Enumerable.B, stage.e2);
		assertEquals("a string", stage.internalName);
		assertEquals(2, stage.requiredInteger);
		assertTrue(stage.map.entrySet().containsAll(map.entrySet()));
		assertFalse(Collections.disjoint(map.entrySet(), stage.map.entrySet()));
		assertTrue(stage.list.containsAll(list));
		assertFalse(Collections.disjoint(list, stage.list));
		assertEquals(query.getAction(), stage.query.getAction());
	}

	@Test(expected = RequiredArgumentMissingException.class)
	public void testSetParameters_throws_RequiredArgumentException_when_required_argument_missing() throws JsonException, RequiredArgumentMissingException, IllegalAccessException {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("string", "string");
		LocalQuery query = new LocalQuery();
		query.requireAction(Action.ADD);
		parameters.put("query", query);

		Map<String, Object> serialized = SerializationUtils.fromJson(SerializationUtils.toJson(parameters));

		TestStage stage = new TestStage();
		stage.setParameters(serialized);
	}

	@Test
	public void testSetParameters_does_not_throw_on_missing_nonrequired_parameters() throws JsonException, RequiredArgumentMissingException, IllegalAccessException {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("requiredInteger", 2);
		LocalQuery query = new LocalQuery();
		query.requireAction(Action.ADD);
		parameters.put("query", query);

		Map<String, Object> serialized = SerializationUtils.fromJson(SerializationUtils.toJson(parameters));

		TestStage stage = new TestStage();
		stage.setParameters(serialized);

		assertEquals(2, stage.requiredInteger);
		assertEquals(query.getAction(), stage.query.getAction());
	}

	@Stage
	static class TestStage extends AbstractStage {
		@Parameter
		private String string;
		
		@Parameter
		private int integer;
		
		public enum Enumerable { A, B, C };
		
		@Parameter
		private Enumerable e1;
		
		@Parameter
		private Enumerable e2;
		
		@Parameter
		private LocalQuery query;

		@Parameter(name = "externalName")
		private String internalName;

		@Parameter(required = true)
		private int requiredInteger;

		@Parameter
		private Map<String, Object> map;

		@Parameter
		private List<String> list;
	}

}
