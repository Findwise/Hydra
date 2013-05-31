package com.findwise.hydra.stage;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.stage.AbstractStageTest.TestStage.Enumerable;

public class AbstractStageTest {

	@Test
	public void testSetParameters() throws Exception {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("string", "string");
		parameters.put("integer", 100);
		parameters.put("e1", "A");
		parameters.put("e2", 1);
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
	}

}
