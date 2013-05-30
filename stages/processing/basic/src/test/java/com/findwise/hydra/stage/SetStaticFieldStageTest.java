package com.findwise.hydra.stage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class SetStaticFieldStageTest {

	@Test
	public void testSetMultiValuedStaticField() throws Exception {
		SetStaticFieldStage stage = new SetStaticFieldStage();
		Map<String, Object> parameters = new HashMap<String, Object>();
		String fieldName = "field_name";
		parameters.put("fieldNames", Arrays.asList(fieldName, fieldName));
		List<String> values = Arrays.asList("value1", "value1");
		parameters.put("fieldValues", values);
		stage.setParameters(parameters);
		LocalDocument doc = new LocalDocument();
		stage.process(doc);
		Assert.assertArrayEquals(values.toArray(), (Object[]) doc.getContentField(fieldName));
	}
}
