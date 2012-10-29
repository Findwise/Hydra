/*
 * Copyright 2012 thomas.gabrielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import static org.junit.Assert.assertEquals;
import org.junit.*;

/**
 * @author thomas.gabrielsen
 */
public class CopyStageTest {
    
    public CopyStageTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Tests whether the input field can be successfully copied to the output
     * field (without the use of pre-/postfixes).
     */
    @Test
    public void testCopyField() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("test_content", "TESTING 1-2-3!");
        String fromField = "test_content";
        String toField = "content";
        CopyStage instance = new CopyStage();
        instance.processField(doc, fromField, toField);
        assertEquals("TESTING 1-2-3!", doc.getContentField(toField));
    }
    
    /**
     * Tests a field copy with added prefix.
     */
    @Test
    public void testCopyFieldWithPrefix() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("test_content", "TESTING 1-2-3!");
        String fromField = "content";
        String toField = "content";
        CopyStage instance = new CopyStage();
        instance.setPrefix("test_");
        instance.processField(doc, fromField, toField);
        assertEquals("TESTING 1-2-3!", doc.getContentField(toField));
    }
    
    /**
     * Tests a field copy with added postfix.
     */
    @Test
    public void testCopyFieldWithPostfix() throws Exception {
        LocalDocument doc = new LocalDocument();
        doc.putContentField("test_content", "TESTING 1-2-3!");
        String fromField = "test_";
        String toField = "content";
        CopyStage instance = new CopyStage();
        instance.setPostfix("content");
        instance.processField(doc, fromField, toField);
        assertEquals("TESTING 1-2-3!", doc.getContentField(toField));
    }
}
