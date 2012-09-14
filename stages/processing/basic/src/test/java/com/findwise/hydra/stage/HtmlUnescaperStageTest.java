package com.findwise.hydra.stage;


import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;
import java.util.LinkedList;
import java.util.List;

public class HtmlUnescaperStageTest {

    @Test
    public void testescape() throws ProcessException {

        HtmlUnescaperStage s = new HtmlUnescaperStage();

        List<String> input = new LinkedList<String>();
        input.add("b");
        input.add("c");

        s.setInputFields(input);

        LocalDocument ld = new LocalDocument();
        ld.putContentField("b", "&oslash; &#248;");
        ld.putContentField("c", "hej hej");
        s.process(ld);

        assert(ld.getContentField("b").equals("ø ø"));
        assert(ld.getContentField("c").equals("hej hej"));
    }
   
        
}
