package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AbstractProcessStageMapperTest {
	Logger logger = LoggerFactory.getLogger(AbstractProcessStageMapperTest.class);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void itWorksForABasicExample() throws Exception {
		String stagePropertiesJsonString = "{\n" +
				"        stageClass: \"" + TestStage.class.getName() + "\",\n" +
				"        query: {\"touched\" : {\"extractTitles\" : true}, \"exists\" : {\"source\" : true} },\n" +
				"        fieldValueMap: {\"source\": [\"value1\", \"value2\"] }\n" +
				"}";
		TestStage stage = (TestStage) AbstractProcessStageMapper.fromJsonString(stagePropertiesJsonString);

		assertThat(stage.getQuery().getTouched().get("extractTitles"), equalTo(true));
		assertThat(stage.getNumberOfThreads(), equalTo(1));
		assertThat(stage.getFieldValueMap().get("source").get(0), equalTo("value1"));
		assertThat(stage.getNonEssentialField(), equalTo(null));
		assertTrue("Stage was not initialized", stage.isInitialized());
	}

	@Test
	public void itThrowsRequiredArgumentMissingExceptionIfStageClassIsMissing() throws Exception {
		expectedException.expect(RequiredArgumentMissingException.class);
		AbstractProcessStageMapper.fromJsonString("{}");
	}

	@Test
	public void itThrowsRequiredArgumentMissingExceptionWhenRequiredParametersAreMissing() throws Exception {
		expectedException.expect(RequiredArgumentMissingException.class);
		expectedException.expectMessage("fieldValueMap");
		String stagePropertiesJsonString = "{\n" +
				"        stageClass: \"" + TestStage.class.getName() + "\"" +
				"}";
		AbstractProcessStageMapper.fromJsonString(stagePropertiesJsonString);
	}

	@Stage
	public static class TestStage extends AbstractProcessStage {
		@Parameter(required = true)
		Map<String, List<String>> fieldValueMap;

		@Parameter(required = false)
		String nonEssentialField;

		private boolean initialized = false;

		@Override
		public void process(LocalDocument document) throws ProcessException {}

		@Override
		public void init() throws RequiredArgumentMissingException, InitFailedException {
			initialized = true;
		}

		public Map<String, List<String>> getFieldValueMap() {
			return fieldValueMap;
		}

		public String getNonEssentialField() {
			return nonEssentialField;
		}

		public boolean isInitialized() {
			return initialized;
		}
	}
}
