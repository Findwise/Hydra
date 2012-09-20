package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import java.util.LinkedList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Roar Granevang
 */
public class RemoveFieldsTest {
    
    public RemoveFieldsTest() {
    }


    @Test
    public void testProcess() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Supertitle");
        doc.putContentField("type", "Supertype");
        
        LinkedList<String> removeFields = new LinkedList<String>();
        removeFields.add("title");
        removeFields.add("otherObject");
        removeFields.add("field_not_existing");
        
        doc.putContentField("otherObject", removeFields);
        
        assertNotNull("not remove yet", doc.getContentField("title"));
        assertNotNull("not remove yet", doc.getContentField("otherObject"));
        RemoveFields stage = new RemoveFields();
        stage.setRemoveFields(removeFields);
        stage.init();
        stage.process(doc);
        
        assertNull("should be removed", doc.getContentField("title"));
        assertNull("should be removed", doc.getContentField("otherObject"));
        
        
   
    }

}
