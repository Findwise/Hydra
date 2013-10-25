package com.findwise.hydra.stage;

import ch.qos.logback.classic.Level;
import com.findwise.hydra.Logging;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalDocumentID;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractProcessStageTest {

	private AbstractProcessStage stage;
	RemotePipeline rp;

	@Before
	public void setUp() throws Exception {
		Logging.setGlobalLoggingLevel(Level.OFF);
	}

	@After
	public void tearDown() {

	}

	@Test
	public void testDocumentProcessException() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = new LimitedCountExceptionStage(3);
		stage.setRemotePipeline(rp);
		spy(stage);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");
		when(testDoc1.getID()).thenReturn(new LocalDocumentID("doc1"));

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.start();
		while (stage.isAlive()) {
			Thread.sleep(10);
		}

		verify(rp, atLeast(3)).getDocument(any(LocalQuery.class));
		verify(rp, atLeast(3)).saveCurrentDocument();
		verify(rp, never()).markFailed(testDoc1);
		verify(rp, never()).markFailed(any(LocalDocument.class), any(Throwable.class));
		verify(testDoc1, atLeast(3)).addError(any(String.class), any(Throwable.class));
	}

	@Test
	public void testFailDocumentOnProcessException() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = new LimitedCountExceptionStage(3);
		stage.setRemotePipeline(rp);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("failDocumentOnProcessException", true);
		stage.setParameters(map);

		spy(stage);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.start();

		while (stage.isAlive()) {
			Thread.sleep(10);
		}

		verify(rp, times(3)).markFailed(any(LocalDocument.class), any(ProcessException.class));
	}

	@Test
	public void testPersistError() throws Exception {
		stage = new SingleDocumentExceptionStage();
		stage.setName("stagename");
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.start();

		while (stage.isAlive()) {
			Thread.sleep(10);
		}

		verify(ld, times(1)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	@Test
	public void testPersistErrorOnSaveFailure() throws Exception {
		RemotePipeline rp = mock(RemotePipeline.class);
		when(rp.getDocument(any(LocalQuery.class))).thenReturn(new LocalDocument());

		when(rp.save(any(LocalDocument.class))).thenReturn(false);
		when(rp.markFailed(any(LocalDocument.class), any(Throwable.class))).thenReturn(false);

		stage = new ImmediateStoppingStage();
		stage.setRemotePipeline(rp);
		stage.start();

		while (stage.isAlive()) {
			Thread.sleep(10);
		}

		verify(rp, times(1)).saveCurrentDocument();
		verify(rp, times(1)).markFailed(any(LocalDocument.class), any(Throwable.class));
	}

	@Test
	public void testProcess_execution_timeout_fails_document() throws Exception {
		stage = new InterruptibleWaitingStage();
		stage.setName(stage.getClass().getCanonicalName());
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("processingTimeout", 1);
		stage.setParameters(map);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);
		stage.start();

		while (stage.isAlive()) {
			Thread.sleep(10);
		}
		verify(ld, times(1)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	@Test
	public void testProcess_execution_timeout_can_continue_processing() throws Exception {
		stage = new LimitedCountInterruptibleWaitingStage(3);
		stage.setName(stage.getClass().getCanonicalName());
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("processingTimeout", 1);
		stage.setParameters(map);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);
		stage.start();

		while (stage.isAlive()) {
			Thread.sleep(10);
		}

		verify(ld, times(3)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	// TODO: Test that stages shut down when processing threads cannot be stopped
}
