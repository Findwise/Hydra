package com.findwise.utils.tika;

import org.apache.tika.metadata.Metadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;
import java.util.List;

import static org.junit.Assert.fail;

public class AttachmentParserTest {

    private AttachmentParser attachmentParser;

    @Before
    public void setUp() {
        attachmentParser = new AttachmentParser();
    }
}
