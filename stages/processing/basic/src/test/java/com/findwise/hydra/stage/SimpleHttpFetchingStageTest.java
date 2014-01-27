package com.findwise.hydra.stage;

import com.findwise.hydra.local.IncorrectFieldTypeException;
import com.findwise.hydra.local.LocalDocument;
import org.apache.commons.io.IOUtils;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SimpleHttpFetchingStageTest extends AbstractHttpFetchingProcessStageTest {

    @Override
    public void setUpStage() throws RequiredArgumentMissingException, IllegalAccessException {
        stage = new SimpleHttpFetchingStage();
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("outputField", "out");
        stage.setParameters(parameters);
    }

    @Override
    protected InputStream getContentStream() {
        return IOUtils.toInputStream("content string");
    }

    @Override
    protected String getStreamEncoding() {
        return "UTF-8";
    }

    @Override
    protected String createTestIdentifier(String identifier) {
        return identifier;
    }

    @Test
    public void testAddsContentToOutput() throws ProcessException, IncorrectFieldTypeException {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("url", "testurl");
        stage.process(doc);
        assertThat(doc.getContentFieldAsString("out"), equalTo("content string"));
    }
}
