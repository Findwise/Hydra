package com.findwise.utils.tika;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TextSanitizerTest {

    private TextSanitizer textSanitizer;

    @Before
    public void setUp() {
        textSanitizer = new TextSanitizer();
    }

    @Test
    public void testFilterString() throws Exception {
        String s = new String(new byte[] {-53});
        System.out.println("char: "+s);
        Assert.assertEquals("", textSanitizer.filterInvalidChars(s));

        List<String> list = new ArrayList<String>();
        list.add("normal");
        list.add("string with some cool unicode\u2603");
        list.add("broken"+new String(new byte[]{-30, -3, -123}));
        list.add("string \u0000with\u0000 NUL\u0000");

        Assert.assertEquals("normal", textSanitizer.filterInvalidChars(list).get(0));
        Assert.assertEquals("string with some cool unicode\u2603", textSanitizer.filterInvalidChars(list).get(1));
        Assert.assertEquals("broken", textSanitizer.filterInvalidChars(list).get(2));
        Assert.assertEquals("string with NUL", textSanitizer.filterInvalidChars(list).get(3));
    }
}
