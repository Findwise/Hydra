package com.findwise.hydra.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.SerializationUtils;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class RemotePipelineTest {

	RemotePipeline rp;

	LocalDocument doc;

	private static final String stageName = "teststage";
	private static final String mockHost = "localhost";
	private static final int mockPort = 37778;

	@ClassRule
	public static WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(mockPort));

	@Before
	public void setUp() {
		rp = new RemotePipeline(mockHost, mockPort, stageName);
		doc = new LocalDocument();
		doc.setID(new LocalDocumentID("testdoc"));
	}

	@Test
	public void testGetFileNames() throws IOException {
		String fileUrl = "/" + RemotePipeline.FILE_URL
				+ "?" + RemotePipeline.STAGE_PARAM + "=" + stageName
				+ "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(doc.getID().toJSON(), "UTF-8");

		stubFor(get(urlEqualTo(fileUrl)).willReturn(aResponse().withBody("[\"file1\", \"file2\"]")));

		List<String> expected = Arrays.asList("file1", "file2");
		List<String> actual = rp.getFileNames(doc.getID());
		assertEquals(expected, actual);
	}

	@Test
	public void testGetFile() throws IOException {
		String fileName = "file1";
		byte[] bytes = "file content".getBytes("UTF-8");
		Date date = new Date();
		String encoding = "UTF-8";
		String mimetype = "text";
		stubFile(fileName, doc.getID(), bytes, date, encoding, mimetype);

		DocumentFile<Local> expected = new DocumentFile<Local>(doc.getID(), fileName, new ByteArrayInputStream(bytes), stageName, date);
		expected.setEncoding(encoding);
		expected.setMimetype(mimetype);
		DocumentFile<Local> actual = rp.getFile(fileName, doc.getID());
		documentFileEquals(expected, actual);
	}

	@Test
	public void testGetFiles() throws IOException {
		Date date = new Date();
		String encoding = "UTF-8";
		String mimetype = "text";
		Map<String, byte[]> testFiles = new HashMap<String, byte[]>();
		testFiles.put("file1", "file1 contents".getBytes(encoding));
		testFiles.put("file2", "contents of file2".getBytes(encoding));

		String fileNamesUrl = "/" + RemotePipeline.FILE_URL
				+ "?" + RemotePipeline.STAGE_PARAM + "=" + stageName
				+ "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(doc.getID().toJSON(), "UTF-8");

		stubFor(get(urlEqualTo(fileNamesUrl)).willReturn(aResponse().withBody("[\"file2\", \"file1\"]")));

		List<DocumentFile<Local>> expected = new ArrayList<DocumentFile<Local>>();
		for (Map.Entry<String, byte[]> testFile : testFiles.entrySet()) {
			final String fileName = testFile.getKey();
			final byte[] content = testFile.getValue();
			stubFile(fileName, doc.getID(), content, date, encoding, mimetype);
			DocumentFile<Local> df = new DocumentFile<Local>(doc.getID(), fileName, new ByteArrayInputStream(content), stageName, date);
			df.setEncoding(encoding);
			df.setMimetype(mimetype);
			expected.add(df);
		}

		List<DocumentFile<Local>> actual = rp.getFiles(doc.getID());

		assertEquals(expected.size(), actual.size());

		for (DocumentFile<Local> expectedDf : expected) {
			DocumentFile<Local> actualDf = actual.get(actual.indexOf(expectedDf));
			documentFileEquals(expectedDf, actualDf);
		}
	}

	private void stubFile(String fileName, LocalDocumentID docId, byte[] content, Date date, String encoding, String mimetype) throws UnsupportedEncodingException {
		String fileUrl = "/" + RemotePipeline.FILE_URL
				+ "?" + RemotePipeline.STAGE_PARAM + "=" + stageName
				+ "&" + RemotePipeline.FILENAME_PARAM + "=" + fileName
				+ "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(docId.toJSON(), "UTF-8");

		Map<String, Object> fileMap = new HashMap<String, Object>();
		fileMap.put("uploadDate", date);
		fileMap.put("encoding", encoding);
		fileMap.put("mimetype", mimetype);
		fileMap.put("savedByStage", stageName);
		fileMap.put("stream", Base64.encodeBase64String(content));
		stubFor(get(urlEqualTo(fileUrl)).willReturn(aResponse().withBody(SerializationUtils.toJson(fileMap))));
	}

	private void documentFileEquals(DocumentFile<Local> expected, DocumentFile<Local> actual) throws IOException {
		assertEquals(expected, actual);
		assertTrue(IOUtils.contentEquals(expected.getStream(), actual.getStream()));
	}
}
