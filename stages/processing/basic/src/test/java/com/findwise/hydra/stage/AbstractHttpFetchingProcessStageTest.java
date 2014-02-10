package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.utils.http.HttpFetcher;
import com.findwise.utils.http.RequestProvider;
import com.findwise.utils.http.UriProvider;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author olof.nilsson@findwise.com
 */
@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractHttpFetchingProcessStageTest {

	protected AbstractHttpFetchingProcessStage stage;
	@Mock
	protected LocalDocument doc;
	@Mock
	protected HttpEntity entity;
	@Mock
	protected Header encodingHeader;
	@Mock
	protected HttpFetcher fetcher;

	abstract public void setUpStage() throws RequiredArgumentMissingException, IllegalAccessException;

	abstract protected InputStream getContentStream();

	abstract protected String getStreamEncoding();

	abstract protected String createTestIdentifier(String identifier);

	@Before
	public void setUp() throws IOException, RequiredArgumentMissingException, IllegalAccessException {
		setUpStage();
		when(encodingHeader.getValue()).thenReturn(getStreamEncoding());
		when(entity.getContent()).thenAnswer(new ContentStreamAnswer(this));
		when(entity.getContentEncoding()).thenReturn(encodingHeader);
		when(fetcher.fetch(anyString(), anyString(), any(UriProvider.class), any(RequestProvider.class))).thenReturn(entity);
		stage.setFetcher(fetcher);
	}

	@Test
	public void testProcess_calls_fetch()
			throws ProcessException, IOException,
			URISyntaxException {
		doc = new LocalDocument();
		String testIdentifier = createTestIdentifier("someidentifier");
		doc.putContentField("url", testIdentifier);
		stage.process(doc);
		verify(fetcher).fetch(testIdentifier, stage.getAcceptedContentHeader(), stage, stage);
	}

	@Test
	public void testProcess_calls_client_with_request_for_url_list()
			throws ProcessException, IOException,
			URISyntaxException {
		doc = new LocalDocument();
		List<String> urls = Arrays.asList(
				createTestIdentifier("someidentifier1"),
				createTestIdentifier("someidentifier2"),
				createTestIdentifier("someidentifier3"));
		doc.putContentField("url", urls);
		stage.process(doc);
		for (String testIdentifier : urls) {
			verify(fetcher).fetch(testIdentifier, stage.getAcceptedContentHeader(), stage, stage);
		}
	}

	@Test
	public void testProcess_adds_ignored_identifier_to_mapped_field() throws
			ProcessException,
			IOException,
			URISyntaxException {
		doc = new LocalDocument();
		List<String> urls = Arrays.asList(
				createTestIdentifier("someidentifier1"),
				createTestIdentifier("someidentifier2"));
		doc.putContentField("url", urls);
		doc.putContentField("some_output_field", "not an identifier");
		Map<String, String> ignored = new HashMap<String, String>();
		ignored.put("someidentifier1", "some_output_field");
		stage.setIgnoredIdentifiers(ignored);
		stage.process(doc);
		assertEquals(Arrays.asList("not an identifier", "someidentifier1"),
				doc.getContentField("some_output_field"));
	}

	protected class ContentStreamAnswer implements Answer<InputStream> {

		AbstractHttpFetchingProcessStageTest test;

		public ContentStreamAnswer(AbstractHttpFetchingProcessStageTest test) {
			this.test = test;
		}

		@Override
		public InputStream answer(InvocationOnMock invocation) throws Throwable {
			return test.getContentStream();
		}
	}
}
