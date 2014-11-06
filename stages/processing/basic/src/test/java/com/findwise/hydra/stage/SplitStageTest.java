package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class SplitStageTest {
    private SplitStage stage;

    @Before
    public void setUp() {
        stage = new SplitStage();
        stage.setInField("in");
        stage.setOutField("out");
        stage.setSplitRegex(",");
    }

    @Test
    public void testProcess() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("in", "Private,Enterprise");

        stage.process(doc);

        assertThat(doc.getContentFieldAsStrings("out"), equalTo(
            Arrays.asList("Private", "Enterprise")));
        assertThat(doc.getContentFieldAsString("in"), equalTo("Private,Enterprise"));
    }

    @Test
    public void emptyField() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("in", "");

        stage.process(doc);

        assertThat(doc.getContentFieldAsStrings("out"), equalTo(Arrays.asList("")));
        assertThat(doc.getContentFieldAsString("in"), equalTo(""));
    }

    @Test(expected = ProcessException.class)
    public void missingFieldEnabledFailOnMissing() throws Exception {
        stage.setFailOnMissing(true);
        stage.process(new LocalDocument());
    }

    @Test
    public void missingFieldDisabledFailOnMissing() throws Exception {
        stage.setFailOnMissing(false);
        stage.process(new LocalDocument());
    }
}