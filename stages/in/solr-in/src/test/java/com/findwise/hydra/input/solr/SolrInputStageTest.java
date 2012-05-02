package com.findwise.hydra.input.solr;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.http.HttpException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class SolrInputStageTest {

	private static SolrInputStage inputStage;
	private static RemotePipeline rp;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		rp = mock(RemotePipeline.class);
		inputStage = new SolrInputStage();
		inputStage.setIdField("id");
		inputStage.setRemotePipeline(rp);
	}

	@Test
	public void testAddRequest() throws IOException,
			HttpException, JsonException {
		String request = "<add><doc><field name=\"foo\">bar</field></doc></add>";
		inputStage.handleBody(request);
		verify(rp).saveFull(
				argThat(new ArgumentMatcher<LocalDocument>() {

					@Override
					public boolean matches(Object arg0) {
						LocalDocument doc = (LocalDocument) arg0;
						if (!"bar".equals(doc.getContentField("foo")))
							return false;
						if (doc.getAction() != Action.ADD)
							return false;
						return true;
					}
				}));
	}
	
	@Test
	public void testDeleteRequest() throws IOException, HttpException, JsonException {
		String request = "<delete><id>foo</id></delete>";
		inputStage.handleBody(request);
		verify(rp).saveFull(
				argThat(new ArgumentMatcher<LocalDocument>() {

					@Override
					public boolean matches(Object arg0) {
						LocalDocument doc = (LocalDocument) arg0;
						if (!"foo".equals(doc.getContentField("id")))
							return false;
						if (doc.getAction() != Action.DELETE)
							return false;
						return true;
					}
				}));
	}

}
