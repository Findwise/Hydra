package com.findwise.hydra;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class StageGroupTest {

	@Mock private DatabaseFile databaseFile;

	private StageGroup stageGroup;

	@Before
	public void setUp() throws Exception {
		stageGroup = new StageGroup("testgroup");
	}

	@Test
	public void testAddStage_replaces_configuration() {
		Stage stageConfig1 = new Stage("stage1", databaseFile);
		stageConfig1.setProperties(new HashMap<String, Object>());
		stageConfig1.setPropertiesModifiedDate(new Date(1000));
		Stage stageConfig2 = new Stage("stage1", databaseFile);
		stageConfig2.setProperties(new HashMap<String, Object>());
		stageConfig2.setPropertiesModifiedDate(new Date(2000));

		stageGroup.addStage(stageConfig1);
		stageGroup.addStage(stageConfig2);
		
		assertEquals(1, stageGroup.getSize());
		assertEquals(stageConfig2, stageGroup.getStage("stage1"));
	}
}
