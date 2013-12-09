package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.junit.Test;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentFileRepository;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.local.Local;
import com.findwise.hydra.local.LocalDocument;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TikaStageTest {
	@Test
	public void testByDefaultExtractsContentMetadataAndLanguage() throws Exception {
		TikaStage stage = new TikaStage();
		LocalDocument doc = buildDocumentWithResourceFile("/test.pdf");
		stage.process(doc);

		assertThat(doc.getContentFieldAsString("test_pdf_Author"), equalTo("Bertrand Delacrétaz"));
		assertThat(doc.getContentFieldAsString("test_pdf_content"), containsString("Tika is a toolkit for detecting and extracting metadata and structured text content"));
		assertThat(doc.getContentFieldAsString("test_pdf_language"), equalTo("en"));
	}

	@Test
	public void testMetadataExtractionCanBeDisabled() throws Exception {
		TikaStage stage = new TikaStage();
		stage.setAddMetaData(false);
		LocalDocument doc = buildDocumentWithResourceFile("/test.pdf");
		stage.process(doc);
		assertFalse("Document should not contain metadata", doc.hasContentField("test_pdf_Author"));
	}

	@Test
	public void testLanguageDetectionCanBeDisabled() throws Exception {
		TikaStage stage = new TikaStage();
		stage.setAddLanguage(false);
		LocalDocument doc = buildDocumentWithResourceFile("/test.pdf");
		stage.process(doc);
		assertFalse("Document should not contain language", doc.hasContentField("test_pdf_language"));
	}

	/**
	 * Creates a LocalDocument containing an attached DocumentFile whose contents are taken from
	 * the given resource.
	 *
	 */
	private LocalDocument buildDocumentWithResourceFile(String resourcePath) throws IOException, URISyntaxException {
		LocalDocument doc = new LocalDocument();
		doc.setDocumentFileRepository(buildDocumentFileRepositoryWithResource(resourcePath));
		return doc;
	}

	/**
	 *  Returns a mock implementation of a DocumentFileRepository containing a single file.
	 *
	 *  The contents of the file are read using this.getClass().getResourceAsStream(resourcePath)
	 */
	private DocumentFileRepository buildDocumentFileRepositoryWithResource(String resourcePath) throws IOException, URISyntaxException {
		String fileName = resourcePath.replaceFirst(".*/", "");
		InputStream inputStream = this.getClass().getResourceAsStream(resourcePath);
		assertThat("Could not find resource with path: " + resourcePath, inputStream, notNullValue());

		// Mock a DocumentFile that has the proper input stream.
		DocumentFile<Local> file = mock(DocumentFile.class);
		when(file.getStream()).thenReturn(inputStream);

		// Mock the fileRepository and make it return the proper file name and our mocked DocumentFile
		DocumentFileRepository fileRepository = mock(DocumentFileRepository.class);
		when(fileRepository.getFileNames(any(DocumentID.class))).thenReturn(Arrays.asList(fileName));
		when(fileRepository.getFile(anyString(), any(DocumentID.class))).thenReturn(file);

		return fileRepository;
	}
}
