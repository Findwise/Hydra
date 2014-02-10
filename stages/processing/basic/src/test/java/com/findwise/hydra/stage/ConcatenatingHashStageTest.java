/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

import static org.junit.Assert.fail;

/**
 *
 * @author poserdonut
 */
public class ConcatenatingHashStageTest {
    
    public ConcatenatingHashStageTest() {
    }

    /**
     * Test of process method, of class ConcatenatingHashStage.
     */
    @Test
    public void testProcess() throws Exception {
        ConcatenatingHashStage chs = new ConcatenatingHashStage();
                
        List<String> fields = new ArrayList<String>();
        fields.add("content");
        fields.add("title");
        
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("algorithm", "MD5");
        settings.put("output", "md5");
        settings.put("fields", fields);
        
        chs.setParameters(settings);
        chs.init();
        
        testStringMD5(chs, RandomStringUtils.randomAscii(10000), RandomStringUtils.randomAlphanumeric(10000));
        
        for (int i = 0; i < 1000; i++) {
            testStringMD5(chs, RandomStringUtils.random(100), RandomStringUtils.random(100));
        }
    }
    
    private void testStringMD5(ConcatenatingHashStage chs, String s, String s1) throws ProcessException {
        LocalDocument ld = new LocalDocument();
        ld.putContentField("content", s);
        ld.putContentField("title", s1);
        chs.process(ld);

        if (!ld.getContentField("md5").equals(DigestUtils.md5Hex(s+s1))) {
            fail("Output not the same as DigestUtils.md5Hex for s='" + s + "'");
        }
    }
}
