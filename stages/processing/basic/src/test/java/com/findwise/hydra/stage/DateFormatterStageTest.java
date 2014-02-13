package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author magnus.ebbesson
 * @author leonard.saers
 */
public class DateFormatterStageTest {

    DateFormatterStage subject;
    Map<String, Object> settings;
    List<String> fields;

    public DateFormatterStageTest() {
    }

    @Before
    public void setUp() {
        subject = new DateFormatterStage();

        fields = new ArrayList<String>();
        fields.add("date");

        settings = new HashMap<String, Object>();
        settings.put("fields", fields);

    }

    @After
    public void tearDown() {
    }

    @Test
    public void testFromEpochToZuluDate_withUTC()
            throws ProcessException, IllegalArgumentException,
            IllegalAccessException, RequiredArgumentMissingException {

        String epochTest = "1377174053";
        String expected = "2013-08-22T12:20:53Z";
        setupStage();

        LocalDocument ld = new LocalDocument();
        ld.putContentField("date", epochTest);

        subject.process(ld);

        assertEquals(expected, ld.getContentField("date"));
    }


    @Test
    public void testFromEpochToZuluDate_withSetTimeZone()
            throws ProcessException, IllegalArgumentException,
            IllegalAccessException, RequiredArgumentMissingException {

        String epochTest = "1377174053";
        String expected = "2013-08-22T14:20:53Z";
        settings.put("epochTimeZone", "+0200");
        setupStage();

        LocalDocument ld = new LocalDocument();
        ld.putContentField("date", epochTest);

        subject.process(ld);

        assertEquals(expected, ld.getContentField("date"));
    }

    @Test
    public void testISO8601FormatToZuluDate()
            throws IllegalArgumentException, IllegalAccessException,
            ProcessException, RequiredArgumentMissingException {
        String iso8601 = "2012-06-29T10:42:53+02:00";
        String expected = "2012-06-29T08:42:53Z";
        setupStage();

        LocalDocument ld = new LocalDocument();
        ld.putContentField("date", iso8601);

        subject.process(ld);

        assertEquals(expected, ld.getContentField("date"));
    }

    private void setupStage() throws IllegalArgumentException,
            IllegalAccessException,
            RequiredArgumentMissingException {
        settings.put("dateFields", fields);
        subject.setParameters(settings);
        subject.init();
    }
}
