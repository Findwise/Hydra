package com.findwise.hydra.stage;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;

public class AbstractProcessStageTest {

	private AbstractProcessStage stage;
	RemotePipeline rp;

	@Before
	public void setUp() throws Exception {

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
	 /*
	@Test
	public void testReadProperties() {
		AbstractProcessStage stage = getDummyAbstractStage();
		Map<String, List<String>> properties = PropertyHandler.readPropertiesFile("CopyStage");
		stage.setProperties(properties);
		assertEquals("oldfield", PropertyHandler.getFirstProperty(stage.getPropertiesMap(), "inField"));
	}

	@Test
	public void testGetDefaultPipelineHost() {
		assertEquals(RemotePipeline.DEFAULT_HOST, getDummyAbstractStage().getPipelineHost());
	}

	@Test
	public void testGetSpecifiedPipeHost()
			throws RequiredArgumentMissingException {

		AbstractProcessStage stage = getDummyAbstractStage();
		Map<String, List<String>> properties = PropertyHandler.readPropertiesFile("CopyStage");
		stage.setProperties(properties);
		assertEquals("byggare.bob", stage.getPipelineHost());
	}

	@Test
	public void testGetDefaultPipePort() {
		assertEquals(RemotePipeline.DEFAULT_PORT, getDummyAbstractStage()
				.getPipelinePort());
	}

	@Test
	public void testGetSpecifiedPipePort()
			throws RequiredArgumentMissingException {
		AbstractProcessStage stage = getDummyAbstractStage();
		Map<String, List<String>> properties = PropertyHandler.readPropertiesFile("CopyStage");
		stage.setProperties(properties);
		assertEquals(4711, stage.getPipelinePort());
	} */

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

	private AbstractProcessStage getErrorAbstractStage(RemotePipeline rp) {
		AbstractProcessStage stage = new DummyAbstractStage() {
			@Override
			public void process(LocalDocument doc) throws ProcessException {
				throw new ProcessException("err");
			}

		};

		stage.setRemotePipeline(rp);
		return stage;
	}

	@Test
	public void testDocumentProcessException() {
		rp = mock(RemotePipeline.class);
		stage = getErrorAbstractStage(rp);
		spy(stage);

		LocalDocument testDoc1 = new LocalDocument();
		testDoc1.putContentField("testField1", "test1");
		testDoc1.putContentField("testField2", "test2");
		
		try {
			when(rp.getDocument(any(LocalQuery.class))).thenReturn(testDoc1);
			when(rp.releaseLastDocument()).thenReturn(true);
		} catch (Exception e) {
			fail(e.getStackTrace().toString());
		}

		stage.start();
		try {
			Thread.sleep(1000);
			stage.stopStage();
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			fail(e.getStackTrace().toString());
		}

		try {
			verify(rp, atLeast(3)).getDocument(any(LocalQuery.class));
			// verify(stage, atLeast(3)).process(any(LocalDocument.class));
			verify(rp, atLeast(3)).releaseLastDocument();
		} catch (Exception e) {
			fail(e.getStackTrace().toString());
		}
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

}
