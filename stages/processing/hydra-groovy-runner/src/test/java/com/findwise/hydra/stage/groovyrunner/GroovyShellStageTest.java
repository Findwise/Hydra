package com.findwise.hydra.stage.groovyrunner;

import com.findwise.hydra.local.LocalDocument;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GroovyShellStageTest {

    private GroovyShellStage stage;

    @Before
    public void setUp() {
        stage = new GroovyShellStage();
    }

    @Test
    public void canRunSimpleScript() throws Exception {
        String script = "doc.putContentField(\"somefield\", \"somevalue\")";
        stage.setScriptText(script);
        stage.init();
        LocalDocument doc = new LocalDocument();
        stage.process(doc);
        Assert.assertThat(doc.hasContentField("somefield"), CoreMatchers.is(true));
        Assert.assertThat(doc.getContentFieldAsString("somefield"), CoreMatchers.is("somevalue"));
    }

    @Test
    public void canRunComplexScript() throws Exception {
        String script =
                "if (doc.hasContentField(\"conditionalfield\") " +
                        "&& doc.getContentFieldAsType(\"conditionalfield\", Boolean.class)) {" +
                "doc.putContentField(\"somefield\", \"somevalue\")\n" +
                "doc.appendToContentField(\"somefield\", \"yetanothervalue\")\n" +
                "}";
        stage.setScriptText(script);
        stage.init();
        LocalDocument doc = new LocalDocument();
        stage.process(doc);
        Assert.assertThat(doc.hasContentField("somefield"), CoreMatchers.is(false));
        doc.putContentField("conditionalfield", true);
        stage.process(doc);
        Assert.assertThat(doc.hasContentField("somefield"), CoreMatchers.is(true));
        Assert.assertThat(doc.getContentFieldAsStrings("somefield"), CoreMatchers.hasItems("somevalue", "yetanothervalue"));
    }
}
