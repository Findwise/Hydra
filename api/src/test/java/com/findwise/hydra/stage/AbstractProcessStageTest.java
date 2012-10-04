package com.findwise.hydra.stage;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.common.Logger.Level;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;

public class AbstractProcessStageTest {

	private AbstractProcessStage stage;
	RemotePipeline rp;

	@Before
	public void setUp() throws Exception {
		Logger.setGlobalLoggingLevel(Level.OFF);
	}

	@After
	public void tearDown() {

	}

	private AbstractProcessStage getDummyAbstractStage(RemotePipeline rp) {
		AbstractProcessStage stage = new DummyAbstractStage();
		stage.setRemotePipeline(rp);
		return stage;
	}

	@SuppressWarnings("unused")
	private static AbstractProcessStage getDummyAbstractStage() {
		AbstractProcessStage stage = new DummyAbstractStage();
		return stage;
	}

	private static class DummyAbstractStage extends AbstractProcessStage {
		@Override
		public void process(LocalDocument doc) throws ProcessException {

		}

		@Override
		public void init() {

		}

	}

	@SuppressWarnings("unused")
	private String[] generateArguments(String stageClass, String stageName,
			int n) {
		String[] argNames = generateArgNames(stageClass, stageName, n);
		String[] argValues = generateArgValues(stageClass, stageName, n);
		String[] ret = new String[n];
		ret[0] = argNames[0] + ":" + argValues[0];
		ret[1] = argNames[1] + ":" + argValues[1];
		for (int i = 2; i < argNames.length; i++) {
			ret[i] = argNames[i] + ":" + argValues[i];
		}
		return ret;
	}

	private String[] generateArgNames(String stageClass, String stageName, int n) {
		String ret[] = new String[n];
		ret[0] = "stageClass";
		ret[1] = "stageName";
		for (int i = 2; i < n; i++) {
			ret[i] = "genArg" + i;
		}
		return ret;
	}

	private String[] generateArgValues(String stageClass, String stageName,
			int n) {
		String ret[] = new String[n];
		ret[0] = stageClass;
		ret[1] = stageName;
		for (int i = 2; i < ret.length; i++) {
			ret[i] = "Hoolabadoooola";
		}
		return ret;
	}

	@Test
	public void testDocumentThroughput() {

		rp = mock(RemotePipeline.class);
		stage = getDummyAbstractStage(rp);

		spy(stage);

		LocalDocument testDoc1 = new LocalDocument();
		testDoc1.putContentField("testField1", "test1");
		testDoc1.putContentField("testField2", "test2");

		try {
			when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
			when(rp.saveCurrentDocument()).thenReturn(true);
		} catch (Exception e) {
			fail(e.getStackTrace().toString());
		}

		stage.start();
		try {
			// We need to let the thread run a bit, and then to let if finish
			Thread.sleep(100);
			stage.stopStage();
			Thread.sleep(100);
		} catch (InterruptedException e) {
			fail(e.getStackTrace().toString());
		}

		try {
			verify(rp, atLeast(3)).getDocument(any(LocalQuery.class));
			// verify(stage, atLeast(3)).process(any(LocalDocument.class));
			verify(rp, atLeast(3)).saveCurrentDocument();
		} catch (Exception e) {
			fail(e.getStackTrace().toString());
		}

	}
	
	@Stage
	public static class ErrorAbstractStage extends AbstractProcessStage {
		@Override
		public void process(LocalDocument doc) throws ProcessException {
			throw new ProcessException("err");
		}

	}

	private AbstractProcessStage getErrorAbstractStage(RemotePipeline rp) {
		AbstractProcessStage stage = new ErrorAbstractStage();

		stage.setRemotePipeline(rp);
		return stage;
	}

	@Test
	public void testDocumentProcessException() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = getErrorAbstractStage(rp);
		spy(stage);

		LocalDocument testDoc1 = mock(LocalDocument.class);
		when(testDoc1.getContentField("testField1")).thenReturn("test1");
		when(testDoc1.getContentField("testField1")).thenReturn("test2");

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
		when(rp.releaseLastDocument()).thenReturn(true);

		stage.start();
		Thread.sleep(1000);
		stage.stopStage();
		Thread.sleep(1000);

		verify(rp, atLeast(3)).getDocument(any(LocalQuery.class));
		verify(rp, atLeast(3)).saveCurrentDocument();
		verify(rp, never()).markFailed(testDoc1);
		verify(rp, never()).markFailed(any(LocalDocument.class), any(Throwable.class));
		verify(testDoc1, atLeast(3)).addError(any(String.class), any(Throwable.class));
	}
	
	@Test
	public void testFailDocumentOnProcessException() throws Exception {
		rp = mock(RemotePipeline.class);
		stage = getErrorAbstractStage(rp);
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
		Thread.sleep(1000);
		stage.stopStage();
		Thread.sleep(1000);

		verify(rp, atLeast(3)).markFailed(any(LocalDocument.class), any(ProcessException.class));
	}

	/**
	 * TestStage to see if methods are called.
	 */
	public static class TestStage extends AbstractProcessStage {

		public static boolean hasProcess = false;
		public static boolean hasInit = false;

		@Override
		public void process(LocalDocument doc) throws ProcessException {
			hasProcess = true;
		}

		@Override
		public void init() {
			hasInit = true;

		}
	}
	
	@Test
	public void testPersistError() throws Exception {
		ExceptionStage es = new ExceptionStage();
		es.setName("stagename");
		RemotePipeline rp = mock(RemotePipeline.class);
		es.setRemotePipeline(rp);
		
		LocalDocument ld = mock(LocalDocument.class);

		when(rp.getDocument(any(LocalQuery.class))).thenReturn(ld);
		when(rp.releaseLastDocument()).thenReturn(true);
		
		es.start();
		
		while(es.isAlive()) {
			Thread.sleep(10);
		}
		
		verify(ld, times(1)).addError(eq(es.getStageName()), any(Throwable.class));
		
	}
	
	public static class ExceptionStage extends AbstractProcessStage {

		@Override
		public void process(LocalDocument doc) throws ProcessException {
			try {
				throw new ProcessException("msg");
			} finally {
				stopStage();
			}
		}
	}
}
