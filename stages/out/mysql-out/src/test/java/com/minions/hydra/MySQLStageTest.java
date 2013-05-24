package com.minions.hydra;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class MySQLStageTest {

	private MySQLStage stage;
	
	@Before
	public void setUp() throws Exception {
		stage = new MySQLStage();
	}

	@Test
	public void testCreateQuery() {		
		String[] cols = { "name", "id", "group", "calendar" };		
		assert(stage.createQuery(cols).equals("INSERT INTO null SET  name = ?, id = ?, group = ?, calendar = ?"));
		
		boolean gotcha = false;
		try
		{
			stage.createQuery(new String[]{});
		} catch(IllegalArgumentException ex) {
			gotcha = true;
		}
		
		assertTrue("Empty list not allowed", gotcha);
	}
}
