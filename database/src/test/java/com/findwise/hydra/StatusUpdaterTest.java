package com.findwise.hydra;

import org.junit.Test;
import org.mockito.Mockito;

public class StatusUpdaterTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testRun() throws Exception {
		DatabaseConnector<?> dbc = Mockito.mock(DatabaseConnector.class);
		StatusWriter sw = Mockito.mock(StatusWriter.class);
		Mockito.when(dbc.getStatusWriter()).thenReturn(sw);
		
		StatusUpdater su = new StatusUpdater(dbc, 1);
		Mockito.verify(sw, Mockito.never()).increment(Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
		su.start();
		
		Thread.sleep(500);
		
		su.interrupt();
		
		Mockito.verify(sw, Mockito.atLeast(2)).increment(Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
		
		Thread.sleep(500);
		Mockito.verifyNoMoreInteractions(sw);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSaveStatus() {
		DatabaseConnector<?> dbc = Mockito.mock(DatabaseConnector.class);
		StatusWriter sw = Mockito.mock(StatusWriter.class);
		Mockito.when(dbc.getStatusWriter()).thenReturn(sw);
		StatusUpdater su = new StatusUpdater(dbc);
		su.addProcessed(3);
		su.addFailed(2);
		su.addDiscarded(1);
		su.saveStatus();
		
		Mockito.verify(sw).increment(3, 2, 1);
		su.saveStatus();
		
		Mockito.verify(sw).increment(0, 0, 0);
		
	}

}
