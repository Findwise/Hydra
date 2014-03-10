package com.findwise.hydra.stage;

import static org.junit.Assert.fail;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;

public class PrioritizedCopyStageTest {
    @Test
    public void testConditionalCopyFalse() throws Exception {

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
    public void testConditionalCopyTrue() throws Exception {

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
    public void testOverwriteEmptyString() throws Exception {

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
    public void testCopyAlreadyExist() throws Exception {

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
    public void testCopyAlreadyButOverWriteIsTrue() throws Exception {

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
    public void testFoundInFirst() throws Exception {

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
    public void testFoundInSecond() throws Exception {

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
    public void testNotFound() throws Exception {

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
        
}
