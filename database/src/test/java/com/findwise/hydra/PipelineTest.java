package com.findwise.hydra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PipelineTest {

	@Test
	public void testEquals() {
		Pipeline p = new Pipeline();
		assertTrue(p.equals(p));
		assertTrue(p.equals(new Pipeline()));
		
		Pipeline p2 = new Pipeline();
		p2.addGroup(new StageGroup("group"));
		assertFalse(p.equals(p2));
		assertFalse(p2.equals(p));
		
		p.addGroup(new StageGroup("group"));
		assertTrue(p.equals(p2));
		
		DatabaseFile df = new DatabaseFile();
		
		p2.getGroup("group").addStage(new Stage("stage", df));
		assertFalse(p.equals(p2));

		p.getGroup("group").addStage(new Stage("stage2", df));
		assertFalse(p.equals(p2));
		
		p2.getGroup("group").addStage(new Stage("stage2", df));
		p.getGroup("group").addStage(new Stage("stage", df));
		
		System.out.println(p);
		System.out.println(p2);
		
		assertTrue(p.equals(p2));
	}

}
