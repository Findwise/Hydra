package com.findwise.hydra.local;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.Document.Action;
import com.findwise.tools.Comparator;
import org.mockito.Mockito;


public class LocalDocumentTest {

	String test1s;
	String test2s;
	LocalDocument test;
	LocalDocument test2;

	@Before
	public void setUp() throws Exception {
		test1s = "{ \"contents\" : { \"name\" : \"test\" , \"number\" : 1} , \"metadata\" : { \"date\" : 1303225121593}}";
		test2s = "{ \"contents\" : { \"name\" : \"test\" , \"number\" : 2} , \"metadata\" : { \"date\" : 1303225121593}}";
		test = new LocalDocument(test1s);
		test2 = new LocalDocument(test2s);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHasContentField() {
		if (!test.hasContentField("name") || !test.hasContentField("number")) {
			fail("Document is missing content field.");
		}
		if (!test2.hasContentField("name") || !test2.hasContentField("number")) {
			fail("Document is missing content field.");
		}
	}

	@Test
	public void testHasMetadataField() {
		if (!test.hasMetadataField("date")) {
			fail("Document is missing metadata field.");
		}
		if (!test2.hasMetadataField("date")) {
			fail("Document is missing metadata field.");
		}
	}

	@Test
	public void testPutContentField() {
		test.putContentField("x", 3);
		if (!test.hasContentField("x")) {
			fail("Content field not found");
		}
		if (!test.getContentField("x").equals(3)) {
			fail("Incorrect value in content field");
		}
	}

	@Test
	public void testRemoveContentField() {
		test.removeContentField("name");
		assertFalse(test.hasContentField("name"));
		assertFalse(test.getContentFields().contains("name"));

		assertTrue(test.getTouchedContent().contains("name"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemovedTransfer() throws Exception {
		test.removeContentField("name");

		Map<String, Object> map = (Map<String, Object>) SerializationUtils.toObject(test.toJson());
		Map<String, Object> content = (Map<String, Object>) map.get(LocalDocument.CONTENTS_KEY);
		assertTrue(content.containsKey("name"));
		assertNull(content.get("name"));
	}

	@Test
	public void testActionTransfer() throws Exception {
		LocalDocument ld = new LocalDocument();

		ld.setAction(Action.DELETE);

		LocalDocument ld2 = new LocalDocument(ld.toJson());
		if (ld.getAction() != ld2.getAction()) {
			fail("Action wasn't serialized, should have been " + ld.getAction() + " but was " + ld2.getAction());
		}

		if (!ld.isEqual(ld2)) {
			fail("Documents not equal");
		}
		ld2.setAction(Action.ADD);
		if (ld.isEqual(ld2)) {
			fail("Documents are equal, even though one is ADD and one is DELETE");
		}
	}

	@Test
	public void testBackslashTransfer() throws Exception {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("test", "escaped \\\\ slash");

		LocalDocument ld2 = new LocalDocument(ld.toJson());

		if (!ld.isEqual(ld2)) {
			fail("Documents not equal");
		}
	}

	@Test
	public void testGetContentField() {
		if (!test.getContentField("number").equals(1) || !test.getContentField("name").equals("test")) {
			fail("Document has wrong value in content field.");
		}
		if (!test2.getContentField("number").equals(2) || !test2.getContentField("name").equals("test")) {
			fail("Document has wrong value in content field.");
		}
	}

	@Test
	public void testGetContentFields() {
		if (test.getContentFields().size() != 2) {
			fail("Incorrect number of contents fields");
		}
		if (!test.getContentFields().contains("name")) {
			fail("No name field found");
		}
		test.putContentField("xyz", "zyx");
		if (test.getContentFields().size() != 3) {
			fail("Incorrect number of contents fields after add");
		}
		if (!test.getContentFields().contains("xyz")) {
			fail("No xyz field found");
		}
	}

	@Test
	public void testJSON() throws Exception {
		String json = test.toJson();
		test.putContentField("number", 2);
		if (json.equals(test.toJson())) {
			fail("Changes to content are not propagated to JSON");
		}
		json = test.toJson();

		test2.putContentField("xyz", "zyx");

		if (test.isEqual(test2)) {
			fail("test2 and test are equal"); //Just a sanity check on equals
		}

		LocalDocument test3 = new LocalDocument(test.toJson());
		if (!test.isEqual(test3)) {
			fail("JSON-generated document is not equal to the JSON source");
		}
	}

	@Test
	public void testEqualsDocument() {
		if (test.isEqual(test2)) {
			fail("Non-equal documents are evaluated as equal");
		}
		test2.putContentField("number", 1);
		if (!test.isEqual(test2)) {
			fail("Two equal documents are evaluated as non-equal");
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFieldsToJSON() throws JsonException {
		List<String> s = Arrays.asList("blahonga");
		String json = test.contentFieldsToJson(s);
		if ((SerializationUtils.fromJson(json)).size() != 2) {
			fail("Did not return correct number of fields. Expected '<contents key>' and '_id'");
		}

		if (!(SerializationUtils.fromJson(json)).containsKey(Document.CONTENTS_KEY)) {
			fail("Did not have a field named CONTENTS_KEY");
		}

		Map<String, Object> m = (Map<String, Object>) (SerializationUtils.fromJson(json)).get(Document.CONTENTS_KEY);
		if (m.size() != 1) {
			fail("Did not return 1 contents field, expected 'blahonga'");
		}

		s.set(0, "name");
		json = test.contentFieldsToJson(s);

		if ((SerializationUtils.fromJson(json)).size() != 2) {
			fail("Did not return correct number of fields. Expected '<contents key>' and '_id'");
		}
		if (!(SerializationUtils.fromJson(json)).containsKey(Document.CONTENTS_KEY)) {
			fail("Did not have a field named <contents key>");
		}
		m = (Map<String, Object>) (SerializationUtils.fromJson(json)).get(Document.CONTENTS_KEY);
		if (!m.containsKey("name")) {
			fail("Did not contain a contents field called 'name'");
		}
		if (!m.get("name").equals("test")) {
			fail("Did not get correct content for field 'name'");
		}

		List<String> s2 = Arrays.asList("name", "number", "blahonga");
		json = test.contentFieldsToJson(s2);
		m = (Map<String, Object>) (SerializationUtils.fromJson(json)).get(Document.CONTENTS_KEY);
		if (m.size() != 3) {
			fail("Did not return correct number of fields. Expected 'test, 'number, 'blahonga' and '_id'");
		}
		if (!m.containsKey("name")) {
			fail("Did not get the field 'name' returned");
		}
		if (!Comparator.equals(m.get("number"), 1)) {
			fail("Did not get correct content for field 'number'");
		}

		test.setAction(Action.DELETE);
		json = test.contentFieldsToJson(s2);

		if (!(SerializationUtils.fromJson(json)).get(Document.ACTION_KEY).equals(Action.DELETE.toString())) {
			fail("Did not get 'action' serialized " + (SerializationUtils.fromJson(json)).get(Document.ACTION_KEY));
		}

	}

	@Test
	public void testIsSynced() throws Exception {
		LocalDocument d = new LocalDocument();
		if (!d.isSynced()) {
			fail("Document should be in sync.");
		}

		d.putContentField("x", "y");
		if (d.isSynced()) {
			fail("Document should be out of sync.");
		}

		d.markSynced();
		if (!d.isSynced()) {
			fail("Document should be in sync again.");
		}

		d.putContentField("x2", "y");
		if (d.isSynced()) {
			fail("Document should be out of sync.");
		}

		LocalDocument d2 = new LocalDocument(d.toJson());
		if (!d2.isSynced()) {
			fail("Document should be in sync.");
		}
	}

	@Test
	public void testHasErrors() throws Exception {
		LocalDocument d = new LocalDocument();

		if (d.hasErrors()) {
			fail("Document shouldn't have errors...");
		}

		d.addError("stage", new NullPointerException("xyz"));
		if (!d.hasErrors()) {
			fail("Document should have errors!");
		}
	}

	@Test
	public void testKeyCantContainPeriodFromJson() throws Exception {
		String test = "{ \"contents\" : { \"name.jens\" : \"test\" , \"number\" : 2} , \"metadata\" : { \"date\" : 1303225121593}}";
		LocalDocument ld = new LocalDocument(test);

		assertFalse(ld.hasContentField("name.jens"));
		assertEquals("test", ld.getContentField("name-jens"));
	}

	@Test
	public void testKeyCantContainPeriodPut() throws Exception {
		String test = "{ \"contents\" : { \"name.jens\" : \"test\" , \"number\" : 2} , \"metadata\" : { \"date\" : 1303225121593}}";
		LocalDocument ld = new LocalDocument(test);
		ld.putContentField("x.y", "");

		assertFalse(ld.hasContentField("x.y"));
	}

	@Test
	public void testAppendToContentField_when_field_is_empty() {
		LocalDocument ld = new LocalDocument();
		ld.appendToContentField("field", new ContentFieldTestType(123));

		List<ContentFieldTestType> expected = new ArrayList<ContentFieldTestType>();
		expected.add(new ContentFieldTestType(123));

		assertTrue(ld.hasContentField("field"));
		assertEquals(expected, ld.getContentField("field"));
	}

	@Test
	public void testAppendToContentField_when_field_has_single_value() {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", new ContentFieldTestType(12));

		ld.appendToContentField("field", new ContentFieldTestType(34));

		List<ContentFieldTestType> expected = new ArrayList<ContentFieldTestType>();
		expected.add(new ContentFieldTestType(12));
		expected.add(new ContentFieldTestType(34));

		assertEquals(expected, ld.getContentField("field"));
	}

	@Test
	public void testAppendToContentField_when_field_is_list() {
		LocalDocument ld = new LocalDocument();
		List<ContentFieldTestType> list = new ArrayList<ContentFieldTestType>();
		list.add(new ContentFieldTestType(12));
		list.add(new ContentFieldTestType(34));
		ld.putContentField("field", list);

		ld.appendToContentField("field", new ContentFieldTestType(56));

		List<ContentFieldTestType> expected = new ArrayList<ContentFieldTestType>();
		expected.add(new ContentFieldTestType(12));
		expected.add(new ContentFieldTestType(34));
		expected.add(new ContentFieldTestType(56));

		assertEquals(expected, ld.getContentField("field"));
	}

	@Test
	public void testGetContentFieldAsString_when_field_is_string() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", "value");

		assertEquals("value", ld.getContentFieldAsString("field"));
	}

	@Test
	public void testGetContentFieldAsStrings_when_field_is_list() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		List<String> list = Arrays.asList("value1", "value2");
		ld.putContentField("field", list);

		List<String> actual = ld.getContentFieldAsStrings("field");
		assertEquals(list, actual);
	}

	@Test
	public void testGetContentFieldAsStrings_when_field_is_empty() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		assertTrue(ld.getContentFieldAsStrings("field_that_does_not_exist").isEmpty());
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsStrings_when_field_is_not_a_list() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		ld.putContentField("wrong_type_field", ContentFieldTestType.class);

		ld.getContentFieldAsStrings("wrong_type_field");
	}

	@Test
	public void testGetContentFieldAsMap_when_field_is_map() throws IncorrectFieldTypeException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("key", "value");
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", map);

		Map<String, Object> actual = ld.getContentFieldAsMap("field");
		assertEquals(map, actual);
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsMap_when_field_is_not_a_map() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field", "I'm a string");

		ld.getContentFieldAsMap("field");
	}

	@Test
	public void testGetContentFieldAsMap_when_field_is_empty() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		assertTrue(ld.getContentFieldAsMap("field_that_does_not_exist").isEmpty());
	}

	@Test
	public void testGetContentFieldAsLong_when_field_is_numeric() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_int", 123);
		ld.putContentField("field_long", 123L);
		ld.putContentField("field_double", 123.4d);

		assertEquals(123L, ld.getContentFieldAsLong("field_int"));
		assertEquals(123L, ld.getContentFieldAsLong("field_long"));
		assertEquals(123L, ld.getContentFieldAsLong("field_double"));

		assertEquals(123L, ld.getContentFieldAsLong("field_int", -1L));
		assertEquals(123L, ld.getContentFieldAsLong("field_long", -1L));
		assertEquals(123L, ld.getContentFieldAsLong("field_double", -1L));
	}

	@Test(expected = FieldIsEmptyException.class)
	public void testGetContentFieldAsLong_when_field_is_empty() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		ld.getContentFieldAsLong("empty_field");
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsLong_with_defaultValue_when_field_is_non_numeric() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_something", ContentFieldTestType.class);

		assertEquals(1234567L, ld.getContentFieldAsLong("field_something", 1234567L));
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsLong_when_field_is_non_numeric() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_something", ContentFieldTestType.class);

		ld.getContentFieldAsLong("field_something");
	}

	@Test
	public void testGetContentFieldAsDouble_when_field_is_numeric() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_int", 123);
		ld.putContentField("field_long", 123L);
		ld.putContentField("field_double", 123.4d);

		double delta = 0.0d;
		assertEquals(123.0d, ld.getContentFieldAsDouble("field_int"), delta);
		assertEquals(123.0d, ld.getContentFieldAsDouble("field_long"), delta);
		assertEquals(123.4d, ld.getContentFieldAsDouble("field_double"), delta);

		assertEquals(123.0d, ld.getContentFieldAsDouble("field_int", -1.0d), delta);
		assertEquals(123.0d, ld.getContentFieldAsDouble("field_long", -1.0d), delta);
		assertEquals(123.4d, ld.getContentFieldAsDouble("field_double", -1.0d), delta);
	}

	@Test(expected = FieldIsEmptyException.class)
	public void testGetContentFieldAsDouble_when_field_is_empty() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		ld.getContentFieldAsDouble("empty_field");
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsDouble_with_defaultValue_when_field_is_non_numeric() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_something", ContentFieldTestType.class);

		assertEquals(1234567.0d, ld.getContentFieldAsDouble("field_something", 1234567.0d), 0.0d);
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsDouble_when_field_is_non_numeric() throws FieldIsEmptyException, IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field_something", ContentFieldTestType.class);

		ld.getContentFieldAsDouble("field_something");
	}

	@Test
	public void testGetContentFieldAsType() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		ld.putContentField("field_string", new ContentFieldTestType(12345));

		ContentFieldTestType actual = ld.<ContentFieldTestType>getContentFieldAsType("field_string", ContentFieldTestType.class);
		assertEquals(12345, actual.prop);
	}

	@Test(expected = IncorrectFieldTypeException.class)
	public void testGetContentFieldAsType_throws_when_field_is_wrong_type() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		ld.putContentField("string_field", "a string");

		ld.getContentFieldAsType("string_field", ContentFieldTestType.class);
	}

	@Test
	public void testGetContentFieldAsType_null_when_field_is_empty() throws IncorrectFieldTypeException {
		LocalDocument ld = new LocalDocument();

		assertNull(ld.getContentFieldAsType("empty_field", String.class));
	}

	public class ContentFieldTestType {

		public int prop;

		public ContentFieldTestType(int prop) {
			this.prop = prop;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ContentFieldTestType that = (ContentFieldTestType) o;

			if (prop != that.prop) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return prop;
		}
	}
}
