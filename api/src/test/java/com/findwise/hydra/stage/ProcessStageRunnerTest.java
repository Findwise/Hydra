package com.findwise.hydra.stage;

import ch.qos.logback.classic.Level;
import com.findwise.hydra.Logging;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalDocumentID;
import com.findwise.hydra.local.RemotePipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeoutException;

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

public class ProcessStageRunnerTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	RemotePipeline rp;

	@Before
	public void setUp() throws Exception {
		Logging.setGlobalLoggingLevel(Level.OFF);
		rp = mock(RemotePipeline.class);
	}

	public ProcessStageRunner buildStageRunner(AbstractProcessStage stage) {
		return new ProcessStageRunner("testStage", stage, rp);
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_does_not_fail_document_when_failDocumentOnProcessException_is_false() throws Exception {
		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws ProcessException {
				throw new ProcessException("Document failed for some reason");
			}
		});

		spy(stageRunner);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");
		when(testDoc1.getID()).thenReturn(new LocalDocumentID("doc1"));

		stageRunner.performProcessing(testDoc1);
		stageRunner.performProcessing(testDoc1);
		stageRunner.performProcessing(testDoc1);

		verify(rp, atLeast(3)).save(testDoc1);
		verify(rp, never()).markFailed(testDoc1);
		verify(rp, never()).markFailed(any(LocalDocument.class), any(Throwable.class));
		verify(testDoc1, atLeast(3)).addError(any(String.class), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_fails_document_on_processException() throws Exception {
		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws ProcessException {
				throw new ProcessException("Stage failed for some reason");
			}

			@Override
			public boolean isFailDocumentOnProcessException() {
				return true;
			}
		});

		spy(stageRunner);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");

		stageRunner.performProcessing(testDoc1);
		stageRunner.performProcessing(testDoc1);
		stageRunner.performProcessing(testDoc1);

		verify(rp, times(3)).markFailed(any(LocalDocument.class), any(ProcessException.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_persists_error() throws Exception {
		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws ProcessException {
				throw new ProcessException("Processing failed for some reason");
			}
		});

		LocalDocument ld = mock(LocalDocument.class);
		stageRunner.performProcessing(ld);

		verify(ld, times(1)).addError(eq("testStage"), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_persists_error_on_save_failure() throws Exception {
		when(rp.save(any(LocalDocument.class))).thenReturn(false);
		when(rp.markFailed(any(LocalDocument.class), any(Throwable.class))).thenReturn(false);

		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws ProcessException {
			}
		});
		stageRunner.performProcessing(new LocalDocument());

		verify(rp, times(1)).save(any(LocalDocument.class));
		verify(rp, times(1)).markFailed(any(LocalDocument.class), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_execution_timeout_fails_document() throws Exception {
		InterruptibleWaitingStage processStage = new InterruptibleWaitingStage();
		processStage.setProcessingTimeout(1);
		ProcessStageRunner stageRunner = buildStageRunner(processStage);

		LocalDocument ld = mock(LocalDocument.class);

		try {
			stageRunner.performProcessing(ld);
		} catch (TimeoutException e) {}

		verify(ld, times(1)).addError(eq("testStage"), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_execution_timeout_throws_timeout_exception() throws Exception {
		exception.expect(TimeoutException.class);
		HangingStage processStage = new HangingStage();
		processStage.setProcessingTimeout(1);
		ProcessStageRunner stageRunner = buildStageRunner(processStage);
		stageRunner.performProcessing(mock(LocalDocument.class));
	}
}
