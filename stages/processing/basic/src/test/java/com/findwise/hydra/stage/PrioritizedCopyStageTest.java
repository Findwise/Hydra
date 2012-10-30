package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import java.util.LinkedList;
import java.util.List;
import junit.framework.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

public class PrioritizedCopyStageTest {

    @Test
    public void testConditionalCopyFalse() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");
        s.setConditionField("type");
        s.setConditionValue("Person Profile");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("type", "SomethingWrong");
        ld.putContentField("b", "bb");
        ld.putContentField("c", "cc");
        s.setConditionField("type");
        s.setConditionValue("Person Profile");
        s.process(ld);

        if (ld.getContentField("a") != null) {
            fail();
        }
    }

    @Test
    public void testConditionalCopyTrue() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");
        s.setConditionField("type");
        s.setConditionValue("Person Profile");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("type", "Person Profile");
        ld.putContentField("b", "bb");
        ld.putContentField("c", "cc");
        s.setConditionField("type");
        s.setConditionValue("Person Profile");
        s.process(ld);

        if (!ld.getContentField("a").equals("bb")) {
            fail();
        }
    }

    @Test
    public void testOverwriteEmptyString() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("copyThis");
        input.add("copyThis2");

        s.setInputFields(input);
        s.setOutputField("emptyfield");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("emptyfield", "");
        ld.putContentField("copyThis", "stuff_to_be_copied");
        ld.putContentField("copyThis2", "more_stuff_to_be_copied");

        s.process(ld);

        Assert.assertEquals("stuff_to_be_copied", ld.getContentField("emptyfield"));
    }

    @Test
    public void testCopyAlreadyExist() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("a", "a");
        s.process(ld);

        if (!ld.getContentField("a").equals("a")) {
            fail();
        }


    }

    @Test
    public void testCopyAlreadyButOverWriteIsTrue() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("add1");
        input.add("add2");

        s.setInputFields(input);
        s.setOutputField("out1");

        s.setOverWrite(true);

        LocalDocument ld = new LocalDocument();
        ld.putContentField("out1", "this_should_be_overwritten");
        ld.putContentField("add1", "this_is_added");
        ld.putContentField("add2", "this_is_not_added_bacause found in 1st");
        s.process(ld);

        Assert.assertEquals("this_is_added", ld.getContentField("out1"));


    }

    @Test
    public void testFoundInFirst() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("b", "bb");
        ld.putContentField("c", "cc");
        s.process(ld);

        if (!ld.getContentField("a").equals("bb")) {
            fail();
        }
    }

    @Test
    public void testFoundInSecond() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("c", "cc");
        s.process(ld);

        if (!ld.getContentField("a").equals("cc")) {
            fail();
        }
    }

    @Test
    public void testNotFound() throws ProcessException {

        PrioritizedCopyStage s = new PrioritizedCopyStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);
        s.setOutputField("a");

        LocalDocument ld = new LocalDocument();
        ld.putContentField("x", "cc");
        s.process(ld);

        if (ld.getContentField("a") != null) {
            fail();
        }
    }

    /**
     * Tests a field copy with added prefix.
     */
    @Test
    public void testCopyFieldWithPrefix() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("test_content", "TESTING 1-2-3!");
        List<String> inFields = new LinkedList<String>();
        inFields.add("content");
        String outField = "content";
        PrioritizedCopyStage instance = new PrioritizedCopyStage();
        instance.setPrefix("test_");
        instance.setInputFields(inFields);
        instance.setOutputField(outField);
        instance.process(doc);
        assertEquals("TESTING 1-2-3!", doc.getContentField(outField));
    }

    /**
     * Tests a field copy with added postfix.
     */
    @Test
    public void testCopyFieldWithPostfix() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("test_content", "TESTING 1-2-3!");
        List<String> inFields = new LinkedList<String>();
        inFields.add("test_");
        String outField = "content";
        PrioritizedCopyStage instance = new PrioritizedCopyStage();
        instance.setPostfix("content");
        instance.setInputFields(inFields);
        instance.setOutputField(outField);
        instance.process(doc);
        assertEquals("TESTING 1-2-3!", doc.getContentField(outField));
    }
}
