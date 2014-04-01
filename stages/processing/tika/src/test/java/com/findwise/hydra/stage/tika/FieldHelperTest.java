package com.findwise.hydra.stage.tika;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FieldHelperTest {

    private Map<String, Object> fields;

    @Before
    public void setUp() {
        fields = new HashMap<String, Object>();
    }

    @Test
    public void testGetUrls() {
        fields.put("title", "title");
        fields.put("attachment_a", "a");
        fields.put("attachment_b", "b");
        fields.put("attachment_c", "c");

        Map<String, Object> ret = FieldHelper.getFieldMatchingPattern(fields, "attachment_(.*)");

        Assert.assertEquals(3, ret.size());
        Assert.assertEquals("a", ret.get("a"));
        Assert.assertEquals("b", ret.get("b"));
        Assert.assertEquals("c", ret.get("c"));
    }

    @Test
    public void testGetUrlsNoGroup() {
        fields.put("title", "title");
        fields.put("attachment_a", "a");
        fields.put("attachment_b", "b");
        fields.put("attachment_c", "c");

        Map<String, Object> ret = FieldHelper.getFieldMatchingPattern(fields, "attachment_.*");

        Assert.assertEquals(3, ret.size());
        Assert.assertEquals("a", ret.get("attachment_a"));
        Assert.assertEquals("b", ret.get("attachment_b"));
        Assert.assertEquals("c", ret.get("attachment_c"));
    }
}
