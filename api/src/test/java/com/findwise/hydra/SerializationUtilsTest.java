package com.findwise.hydra;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializationUtilsTest {

	@Test
	public void testDate_serialization_equality() throws JsonException {
		Date date = new Date();
		Object deserializedDate = SerializationUtils.toObject(SerializationUtils.toJson(date));
		assertEquals(Date.class, deserializedDate.getClass());
		assertTrue(date.compareTo((Date)deserializedDate) == 0);
	}

	@Test
	public void testDates_can_deserialize_legacy_date_format() throws JsonException {
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
		Object deserializedDate = SerializationUtils.toObject("\"" + format.format(date) + "\"");
		assertEquals(Date.class, deserializedDate.getClass());
		// Legacy date format does not handle sub-second time
		assertTrue(date.after((Date)deserializedDate));
		assertTrue(date.getTime() - ((Date)deserializedDate).getTime() < 1000L);
	}

}
