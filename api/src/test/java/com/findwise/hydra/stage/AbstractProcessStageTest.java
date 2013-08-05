package com.findwise.hydra.stage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.Logging;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;

public class AbstractProcessStageTest {
	private RemotePipeline rp;

	@Before
	public void setUp() throws Exception {
        Logging.setGlobalLoggingLevel(Level.OFF);
        rp = mock(RemotePipeline.class);
	}

	@Test
	public void testDocumentProcessException() throws Exception {
		ProcessExceptionStage stage = new ProcessExceptionStage();
		stage.setRemotePipeline(rp);

		LocalDocument testDoc = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc);

		stage.fetchAndProcess();

		verify(rp, times(1)).getDocument(any(LocalQuery.class));
		verify(rp, times(1)).saveCurrentDocument();
		verify(rp, never()).markFailed(testDoc);
		verify(rp, never()).markFailed(any(LocalDocument.class), any(Throwable.class));
		verify(testDoc, times(1)).addError(any(String.class), any(Throwable.class));
	}

	@Test
	public void testFailDocumentOnProcessException() throws Exception {
		ProcessExceptionStage stage = new ProcessExceptionStage();
		stage.setRemotePipeline(rp);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("failDocumentOnProcessException", true);
		stage.setParameters(map);

		LocalDocument testDoc = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc);

		stage.fetchAndProcess();

		verify(rp, times(1)).markFailed(eq(testDoc), any(ProcessException.class));
	}

	@Test
	public void testPersistError() throws Exception {
		ProcessExceptionStage stage = new ProcessExceptionStage();
		stage.setRemotePipeline(rp);
		stage.setName("stagename");
		
		LocalDocument testDoc = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc);
		
		stage.fetchAndProcess();
		
		verify(testDoc, times(1)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	@Test
	public void testPersistErrorOnSaveFailure() throws Exception {
		AbstractProcessStage stage = new NopStage();
		stage.setRemotePipeline(rp);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(new LocalDocument());
		when(rp.save(any(LocalDocument.class))).thenReturn(false);
		when(rp.markFailed(any(LocalDocument.class), any(Throwable.class))).thenReturn(false);

		stage.fetchAndProcess();
		
		verify(rp, times(1)).saveCurrentDocument();
		verify(rp, times(1)).markFailed(any(LocalDocument.class), any(Throwable.class));
	}

	@Test
	public void testActionsToProcess() throws Exception {
		AbstractProcessStage stage = new NopStage();
		stage.setRemotePipeline(rp);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("actionsToProcess", Arrays.asList(Action.ADD.name(), Action.UPDATE.name()));
		stage.setParameters(map);
		stage.init();
		AbstractProcessStage spyStage = spy(stage);
		when(rp.saveCurrentDocument()).thenReturn(true);

		for (Action action : Action.values()) {
			LocalDocument testDoc = new LocalDocument();
			testDoc.setAction(action);
			when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc);

			spyStage.fetchAndProcess();

			if (action == Action.ADD || action == Action.UPDATE) {
				verify(spyStage, times(1)).process(eq(testDoc));
			} else {
				verify(spyStage, never()).process(eq(testDoc));
			}
		}
	}

	@Test
	public void testAllActionsShouldBeProcessedAsDefault() throws Exception {
		AbstractProcessStage stage = new NopStage();
		stage.setRemotePipeline(rp);
		stage.init();
		AbstractProcessStage spyStage = spy(stage);
		when(rp.saveCurrentDocument()).thenReturn(true);

		for (Action action : Action.values()) {
			LocalDocument testDoc = new LocalDocument();
			testDoc.setAction(action);
			when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc);

			spyStage.fetchAndProcess();

			verify(spyStage, times(1)).process(eq(testDoc));
		}
	}

	@Stage
	private static class ProcessExceptionStage extends AbstractProcessStage {
		@Override
		public void process(LocalDocument doc) throws ProcessException {
			throw new ProcessException();
		}
	}

	@Stage
	private static class NopStage extends AbstractProcessStage {
		@Override
		public void process(LocalDocument doc) throws ProcessException {
		}
	}
}
