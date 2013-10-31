package com.findwise.hydra.debugging;

import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class ThrowingStageTest {
	
	ThrowingStage stage = new ThrowingStage();
	
	@Test(expected=RuntimeException.class)
	public void testProcessIsThrowing() throws Exception {
		stage.process(new LocalDocument());
	}
}
