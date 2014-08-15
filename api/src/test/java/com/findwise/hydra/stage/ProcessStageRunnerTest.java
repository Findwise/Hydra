package com.findwise.hydra.stage;

import ch.qos.logback.classic.Level;
import com.findwise.hydra.Logging;
import com.findwise.hydra.local.HttpRemotePipeline;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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
		rp = mock(HttpRemotePipeline.class);
	}

	public ProcessStageRunner buildStageRunner(AbstractProcessStage stage) {
		return new ProcessStageRunner("testStage", stage, rp);
	}

	@Test
	public void testPerformProcessing_fails_document_if_stage_throws_exceptions() throws Exception {
		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws Exception {
				throw new Exception("Stage failed for some reason");
			}
		});

		spy(stageRunner);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		stageRunner.performProcessing(testDoc1);

		verify(rp, times(1)).markFailed(eq(testDoc1), any(Exception.class));
	}

	@Test
	public void testPerformProcessing_persists_error_on_save_failure() throws Exception {
		when(rp.save(any(LocalDocument.class))).thenReturn(false);
		when(rp.markFailed(any(LocalDocument.class), any(Throwable.class))).thenReturn(false);

		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) {}
		});
		stageRunner.performProcessing(new LocalDocument());

		verify(rp, times(1)).save(any(LocalDocument.class));
		verify(rp, times(1)).markFailed(any(LocalDocument.class), any(Throwable.class));
	}

	@Test(timeout = 1000)
	public void testPerformProcessing_throws_timeout_exception_if_timeout_exceeded() throws Exception {
		exception.expect(TimeoutException.class);
		HangingStage processStage = new HangingStage();
		processStage.setProcessingTimeout(1);
		ProcessStageRunner stageRunner = buildStageRunner(processStage);
		stageRunner.performProcessing(mock(LocalDocument.class));
	}

	@Test
	public void testPerformProcessing_does_not_rethrow_timeout_exception_if_thrown_by_stage() throws Exception {
		ProcessStageRunner stageRunner = buildStageRunner(new AbstractProcessStage() {
			@Override
			public void process(LocalDocument doc) throws Exception {
				throw new TimeoutException();
			}
		});
		// There are no asserts here since we are testing that performProcessing will *not*
		// rethrow the TimeoutException.
		stageRunner.performProcessing(mock(LocalDocument.class));
	}
}
