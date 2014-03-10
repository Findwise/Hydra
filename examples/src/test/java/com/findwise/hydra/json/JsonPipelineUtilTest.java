package com.findwise.hydra.json;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author johan.sjoberg
 */
public class JsonPipelineUtilTest {

    private static final String JSON_FILE = "/sample pipeline.json";

    @SuppressWarnings("unchecked")
    @Test
    public void testFromJson_InputStreamReader() throws FileNotFoundException {
        InputStream in = this.getClass().getResourceAsStream(JSON_FILE);
        InputStreamReader reader = new InputStreamReader(in);
        PipelineConfiguration cfg = new JsonPipelineUtil().fromJson(reader);
        assertEquals("contacts", cfg.getPipelineName());
        assertEquals(1, cfg.getStages().size());

        Map<String, Object> stage = cfg.getStages().get(0);
        assertEquals("regexdates", stage.get("stageName"));
        assertEquals("com.findwise.hydra.stage.RegexStage", stage.get("stageClass"));
        assertEquals("basic", stage.get("jarId"));
        assertArrayEquals(new String[]{"touched(mergeFields,true)"}, ((List) stage.get("queryOptions")).toArray());

        List<Map<String, String>> listOfRegexConfigs = (List<Map<String, String>>) stage.get("regexConfigs");
        assertEquals(1, listOfRegexConfigs.size());
        Map<String, String> regexConfig = listOfRegexConfigs.get(0);
        assertEquals(4, regexConfig.size());
        assertEquals("releasedatetime", regexConfig.get("inField"));
        assertEquals("releasedatetime", regexConfig.get("outField"));
        assertEquals("(.*)(\\+\\d{2}):(\\d{2})", regexConfig.get("regex"));
        assertEquals("$1$2$3", regexConfig.get("substitute"));
        System.out.println("* " + cfg.toString());
    }
}
