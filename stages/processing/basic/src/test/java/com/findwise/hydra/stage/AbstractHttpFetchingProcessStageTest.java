package com.findwise.hydra.stage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import junit.framework.Assert;

import com.findwise.hydra.local.LocalDocument;

/**
 * @author olof.nilsson@findwise.com
 */
@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractHttpFetchingProcessStageTest {

    @Mock
    protected AbstractHttpFetchingProcessStage stage;
    @Mock
    protected LocalDocument doc;
    @Mock
    protected HttpClient client;
    @Mock
    protected HttpGet request;
    @Mock
    protected HttpResponse response;
    @Mock
    protected StatusLine status;
    @Mock
    protected HttpEntity entity;
    @Mock
    protected Header encodingHeader;

    protected String content = "content";

    abstract public void setUpStage();

    abstract protected InputStream getContentStream();

    abstract protected String getStreamEncoding();

    abstract protected String createTestIdentifier(String identifier);

    @Before
    public void setUp() throws ClientProtocolException, IOException {
        setUpStage();
        when(encodingHeader.getValue()).thenReturn(getStreamEncoding());
        when(entity.getContent()).thenAnswer(new ContentStreamAnswer(this));
        when(entity.getContentEncoding()).thenReturn(encodingHeader);
        when(response.getEntity()).thenReturn(entity);
        when(status.getStatusCode()).thenReturn(200);
        when(status.getReasonPhrase()).thenReturn("OK");
        when(response.getStatusLine()).thenReturn(status);
        when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
        stage.setCookieStore(new BasicCookieStore());
        stage.setClient(client);
        stage.setRequest(request);
    }

    @Test
    public void testProcess_calls_client_with_request_for_url()
            throws Exception {
        doc = new LocalDocument();
        doc.putContentField("url", createTestIdentifier("someidentifier"));
        stage.process(doc);
        verify(request)
                .setURI(stage
                        .getUriFromIdentifier(createTestIdentifier("someidentifier"), 0));
        verify(client).execute(request);
    }

    @Test
    public void testProcess_calls_client_with_request_for_url_list()
            throws Exception {
        doc = new LocalDocument();
        List<String> urls = Arrays.asList(
                createTestIdentifier("someidentifier1"),
                createTestIdentifier("someidentifier2"));
        doc.putContentField("url", urls);
        stage.process(doc);
        verify(request)
                .setURI(eq(stage
                        .getUriFromIdentifier(createTestIdentifier("someidentifier1"),
                                0)));
        verify(request)
                .setURI(eq(stage
                        .getUriFromIdentifier(createTestIdentifier("someidentifier2"),
                                0)));
        // assigned url
        verify(client, times(2)).execute(request);
    }

    @Test
    public void testProcess_adds_ignored_identifier_to_mapped_field() throws
            Exception {
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
        verify(request)
                .setURI(eq(stage
                        .getUriFromIdentifier(createTestIdentifier("someidentifier2"),
                                0)));
        verify(client).execute(request);
        Assert.assertEquals(Arrays.asList("not an identifier", "someidentifier1"),
                doc.getContentField("some_output_field"));
    }

    @Test
    public void testGetFieldAsStrings_gets_list() {
        doc = new LocalDocument();
        List<String> urls = new ArrayList<String>();
        urls.add("1");
        urls.add("2");
        doc.putContentField("field", urls);
        List<String> list = stage.getFieldAsStrings(doc, "field");
        Assert.assertEquals(urls, list);
    }

    @Test
    public void testGetFieldAsStrings_empty_field_gets_list() {
        doc = new LocalDocument();
        List<String> list = stage.getFieldAsStrings(doc, "field");
        Assert.assertEquals(Collections.EMPTY_LIST, list);
    }

    @Test
    public void testGetFieldAsStrings_string_field_gets_list() {
        doc = new LocalDocument();
        doc.putContentField("field", "something");
        List<String> list = stage.getFieldAsStrings(doc, "field");
        Assert.assertEquals(Arrays.asList("something"), list);
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
