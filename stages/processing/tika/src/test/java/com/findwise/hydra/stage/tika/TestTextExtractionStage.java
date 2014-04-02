package com.findwise.hydra.stage.tika;

import static org.junit.Assert.fail;

import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.RequiredArgumentMissingException;


public class TestTextExtractionStage {

	private TextExtractionStage stage;
	private LocalDocument doc;
	
	@Before
	public void init() {
		stage = new TextExtractionStage();
		
		stage.setUrlField("url");
		stage.setContentField("out");
		stage.setMetadatPrefix("meta_");

		doc = new LocalDocument();
	}
	
	@Test
	public void testInit() throws RequiredArgumentMissingException {
		stage.init();
	}
	
	@Test
	public void testGetUrl() {
		doc.putContentField("url", "http://www.google.com");
		
		String url = stage.getUrl(doc);
		String expected = "http://www.google.com";
		if (!expected.equals(url)) {
			fail("Failed to read url. Expected: " + expected + " Got: " + url);
		}
	}
	
	@Test
	public void testGetUrlFromList() {
		doc.putContentField("url", Arrays.asList(new String[]{"http://www.google.com"}));
		
		String url = stage.getUrl(doc);
		String expected = "http://www.google.com";
		if (!expected.equals(url)) {
			fail("Failed to read url. Expected: " + expected + " Got: " + url);
		}
	}
	
	@Test
	public void testGetUrlFromListWithMoreUrls() {
		doc.putContentField("url", Arrays.asList(new String[]{"http://www.google.com", "http://www.asdf.com"}));
		
		String url = stage.getUrl(doc);
		String expected = "http://www.google.com";
		if (!expected.equals(url)) {
			fail("Failed to read url. Expected: " + expected + " Got: " + url);
		}
	}
	
	@Test
	public void testGetUrlFromNull() {
		String url = stage.getUrl(doc);
		String expected = null;
		if (url != null) {
			fail("Failed to read url. Expected: " + expected + " Got: " + url);
		}
	}
	
	@Test
	public void testGetUrlWithInteger() {
		doc.putContentField("url", new Integer(5));
		String url = stage.getUrl(doc);
		String expected = null;
		if (url != null) {
			fail("Failed to read url. Expected: " + expected + " Got: " + url);
		}
	}
	
	@Test(expected=MalformedURLException.class)
	public void testGetStreamFromInvalidUrl() throws Exception {
		stage.getStreamFromUrl("myInvalidUrl");
	}

	@Test
	public void testAddTextToDocument() {
		String text = "My text";
		
		stage.addTextToDocument(doc, text);
		
		String field = (String)doc.getContentField(stage.getContentField());
		if (!text.equals(field)) {
			fail("Expected: " + text + " got: " + field);
		}
	}
	
	@Test
	public void testAddMetadataToDocument() {
        Map<String, Object> meta = new HashMap<String, Object>();
		meta.put("author", "Simon");
		meta.put("pages", "5");
		
		stage.addMetadataToDocument(doc, meta);
		
		String author = (String)doc.getContentField(stage.getMetadataPrefix() + "author");
		String pages = (String)doc.getContentField(stage.getMetadataPrefix() + "pages");
		
		if (!"Simon".equals(author)) {
			fail("Expected Simon Got: "  + author + " in field " + stage.getMetadataPrefix() + "author");
		}

		if (!"5".equals(pages)) {
			fail("Expected 5 Got: "  + pages + " in field " + stage.getMetadataPrefix() + "pages");
		}

	}

	@Test
	public void testAddMultiValueMetadataToDocument() {
		Map<String, Object> meta = new HashMap<String, Object>();
		meta.put("author", Arrays.asList("Olof", "Simon"));
		
		stage.addMetadataToDocument(doc, meta);
		
		@SuppressWarnings("unchecked")
		List<String> author = (List<String>)doc.getContentField(stage.getMetadataPrefix() + "author");
		
		if (!author.contains("Olof")) {
			fail("Missing Olof in authors");
		}
		
		if (!author.contains("Simon")) {
			fail("Missing Olof in authors");
		}

	}
	
	@Test
	public void testOkFileSize() {
		stage.setMaxSizeInBytes(1024);
		
		if (!stage.okFileSize(123)) {
			fail("Did not approve file with filesize 123 when maxSize was 1024");
		}
		
	}
	
	public void testFileSizeNull() {
		stage.setMaxSizeInBytes(1024);
		
		if (stage.okFileSize(-1)) {
			fail("Approved file with unknown file size when maxSize was 1024");
		}
	}

	@Test
	public void testMaxFileSizeNotSet() {

		if (!stage.okFileSize(-1)) {
			fail("No max size was set, but file was not approved");
		}
	}
	
	@Test
	public void testGetFileSizeFromString() {
		stage.setFileSizeField("size");
		
		doc.putContentField("size", "1024");
		
		long got = stage.getFileSize(doc);
		
		if (got != 1024) {
			fail("Got " + got + " but excpected " + 1024);
		}
	}
	
	@Test
	public void testGetFileSizeFromList() {
		stage.setFileSizeField("size");
		
		doc.putContentField("size", Arrays.asList("1024"));
		
		long got = stage.getFileSize(doc);
		
		if (got != 1024) {
			fail("Got " + got + " but excpected " + 1024);
		}
	}
	
	@Test
	public void testGetFileSizeFromInteger() {
		stage.setFileSizeField("size");
		
		doc.putContentField("size", new Integer(1024));
		
		long got = stage.getFileSize(doc);
		
		if (got != 1024) {
			fail("Got " + got + " but excpected " + 1024);
		}
	}
	
	@Test
	public void testGetFileSizeFromLong() {
		stage.setFileSizeField("size");
		
		doc.putContentField("size", new Long(1024));
		
		long got = stage.getFileSize(doc);
		
		if (got != 1024) {
			fail("Got " + got + " but excpected " + 1024);
		}
	}
	
	@Test
	public void testGetFileSizeFromListLong() {
		stage.setFileSizeField("size");
		doc.putContentField("size", Arrays.asList(new Long(1024)));
		
		long got = stage.getFileSize(doc);
		
		if (got != 1024) {
			fail("Got " + got + " but excpected " + 1024);
		}
	}
	
	@Test
	public void testGetFileFormat() {
		stage.setFileFormatField("fileFormat");

		doc.putContentField("fileFormat", "ppt");
		String format = stage.getFileFormat(doc);
		
		if (!"ppt".equals(format)) {
			fail("File format was not extracted");
		}
	}

	@Test
	public void testGetFileFormatFromList() {
		stage.setFileFormatField("fileFormat");

		doc.putContentField("fileFormat", Arrays.asList("ppt"));
		String format = stage.getFileFormat(doc);
		
		if (!"ppt".equals(format)) {
			fail("File format was not extracted");
		}
	}
	
	@Test
	public void testGetNonExistingFileFormat() {
		stage.setFileFormatField("fileFormat");
		String format = stage.getFileFormat(doc);
		
		if (format != null) {
			fail("File format was not set. Expected null, got " + format);
		}
	}
	
	@Test
	public void testOkFileFormat() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("ppt", "doc"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (!stage.okFileFormat("ppt")) {
			fail("Got false but expected true");
		}
	}
	
	@Test
	public void testOkFileFormat_formatIsUppercase() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("ppt", "doc"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (!stage.okFileFormat("PPT")) {
			fail("Got false but expected true");
		}
	}
	

	@Test
	public void testOkFileFormat_allowedFormatIsUppercase() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("PPT", "DOC"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (!stage.okFileFormat("ppt")) {
			fail("Got false but expected true");
		}
	}
	
	@Test
	public void testOkFileFormatNotFirst() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("ppt", "doc"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (!stage.okFileFormat("doc")) {
			fail("Got false but expected true");
		}
	}

	@Test
	public void testNotOkFileFormat() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("ppt", "doc"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (stage.okFileFormat("jpg")) {
			fail("Got true but expected false");
		}
	}
	

	@Test
	public void testOkFileFormat_allowedFormatsNotSet() throws Exception {
		stage.setLowerCaseAllowedFileFormats();
		if (!stage.okFileFormat("jpg")) {
			fail("allowedFormats was not set, but jpg was not ok anyway");
		}
	}

	@Test
	public void testOkFileFormat_allowedFormatsNotSet_formatIsNull() throws Exception {
		stage.setLowerCaseAllowedFileFormats();
		if (!stage.okFileFormat(null)) {
			fail("allowedFormats was not set, but null was not ok anyway");
		}
	}
	
	@Test
	public void testOkFileFormat_formatIsNull() throws Exception {
		stage.setAllowedFileFormats(Arrays.asList("ppt", "doc"));
		stage.setLowerCaseAllowedFileFormats();
		
		if (stage.okFileFormat(null)) {
			fail("Accepted null format");
		}
	}
}