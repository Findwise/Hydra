package com.findwise.hydra;

import java.io.File;
import java.util.HashMap;

import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.StageRunner.StageDestroyer;

public class StageRunnerTest {

	@Test
	public void testDestroy() throws Exception {
		StageDestroyer sd = new StageDestroyer();
		Process p = Mockito.mock(Process.class);
		sd.add(p);
		
		StageGroup group = new StageGroup("group");
		Stage mockedStage = Mockito.mock(Stage.class);
		group.addStage(mockedStage);
		Mockito.when(mockedStage.getProperties()).thenReturn(new HashMap<String, Object>());
		
		TerminationHandler terminationHandler = Mockito.mock(TerminationHandler.class);
		Mockito.when(terminationHandler.isTerminating()).thenReturn(false);
		
		StageRunner sr = new StageRunner(group, new File("test"), 0, false, 0, terminationHandler);
		sr.setStageDestroyer(sd);
		
		sr.destroy();
		
		Mockito.verify(p, Mockito.times(1)).destroy();
	}

}
