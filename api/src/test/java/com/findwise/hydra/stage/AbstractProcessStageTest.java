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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
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
	private StageKiller mockStageKiller;
	RemotePipeline rp;

	@Before
	public void setUp() throws Exception {
		Logging.setGlobalLoggingLevel(Level.OFF);

		// We don't like System.exit() in tests.
		// TODO: We don't like System.exit() at all :)
		mockStageKiller = mock(StageKiller.class);
	}

	@After
	public void tearDown() {

	}

	@Test(timeout = 1000)
	public void testProcess_does_not_fail_document_when_failDocumentOnProcessException_is_false() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = new LimitedCountExceptionStage(3);
		stage.setRemotePipeline(rp);
		stage.setStageKiller(mockStageKiller);
		spy(stage);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");
		when(testDoc1.getID()).thenReturn(new LocalDocumentID("doc1"));

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.run();

		verify(rp, atLeast(3)).getDocument(any(LocalQuery.class));
		verify(rp, atLeast(3)).saveCurrentDocument();
		verify(rp, never()).markFailed(testDoc1);
		verify(rp, never()).markFailed(any(LocalDocument.class), any(Throwable.class));
		verify(testDoc1, atLeast(3)).addError(any(String.class), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testProcess_fails_document_on_processException() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = new LimitedCountExceptionStage(3);
		stage.setRemotePipeline(rp);
		stage.setStageKiller(mockStageKiller);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("failDocumentOnProcessException", true);
		stage.setParameters(map);

		spy(stage);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.run();

		verify(rp, times(3)).markFailed(any(LocalDocument.class), any(ProcessException.class));
	}

	@Test(timeout = 1000)
	public void testProcess_persists_error() throws Exception {
		stage = new SingleDocumentExceptionStage();
		stage.setName("stagename");
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);
		stage.setStageKiller(mockStageKiller);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.run();

		verify(ld, times(1)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testProcess_persists_error_on_save_failure() throws Exception {
		RemotePipeline rp = mock(RemotePipeline.class);
		when(rp.getDocument(any(LocalQuery.class))).thenReturn(new LocalDocument());

		when(rp.save(any(LocalDocument.class))).thenReturn(false);
		when(rp.markFailed(any(LocalDocument.class), any(Throwable.class))).thenReturn(false);

		stage = new ImmediateStoppingStage();
		stage.setRemotePipeline(rp);
		stage.setStageKiller(mockStageKiller);
		stage.run();

		verify(rp, times(1)).saveCurrentDocument();
		verify(rp, times(1)).markFailed(any(LocalDocument.class), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testProcess_execution_timeout_fails_document() throws Exception {
		stage = new InterruptibleWaitingStage();
		stage.setName(stage.getClass().getCanonicalName());
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);
		stage.setStageKiller(mockStageKiller);

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("processingTimeout", 1);
		stage.setParameters(map);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);
		stage.run();

		verify(ld, times(1)).addError(eq(stage.getStageName()), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testProcess_execution_timeout_kills_stage() throws Exception {
		stage = new HangingStage();
		stage.setName(stage.getClass().getCanonicalName());
		RemotePipeline rp = mock(RemotePipeline.class);
		stage.setRemotePipeline(rp);

		stage.setStageKiller(mockStageKiller);

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("processingTimeout", 1);
		stage.setParameters(map);

		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);
		stage.run();

		verify(mockStageKiller).kill(eq(stage));
	}
}
