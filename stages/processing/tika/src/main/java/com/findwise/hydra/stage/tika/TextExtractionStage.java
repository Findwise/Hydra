package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

@Stage(description = "A stage that fetches the content from a given url and appends it the the document")
public class TextExtractionStage extends AbstractProcessStage {
    private static Logger logger = LoggerFactory.getLogger(TextExtractionStage.class);

    @Parameter(description = "The max size to be fetched. Default: -1 = unlimited")
	private long maxSizeInBytes = -1;
	@Parameter(required = true, description = "The prefix to add to the metadata fields when adding them to the content")
	private String metadataPrefix;
	@Parameter(required = true, description = "The field name where the extracted content will be stored")
	private String contentField;
	@Parameter(required = true, description = "The field where the url can be found")
	private String urlField;
	@Parameter(description = "The field where the file size can be found")
	private String fileSizeField;
	@Parameter(description = "The allowed file formats. Default: null = all")
	private List<String> allowedFileFormats = null;
	@Parameter(description = "The field where the file format can be found")
	private String fileFormatField;

	private Set<String> lowerCaseAllowedFileFormatsSet = null;
	
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		InputStream stream = null;
		long size = getFileSize(doc);

		if (!okFileSize(size)) {
			logger.debug("File size was not ok. Skipping");
			return;
		}

		String fileFormat = getFileFormat(doc);
		if (!okFileFormat(fileFormat)) {
			logger.debug("File format " + fileFormat + " was not an allowed file format");
			return;
		}

		String url = getUrl(doc);

		try {
			stream = getStreamFromUrl(url);
		} catch (IOException e) {
			logger.warn("Failed to open stream to url: " + url, e);
			return;
		}

		try {
			enrichDocumentWithFileContents(doc, stream);
		} catch (Exception e) {
			logger.warn(
					"The parser experienced a problem. " + "The data from the specified file will not be included.", e);
			return;
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				logger.warn("Failed to close stream. Was it never opened?");
			}
		}

	}

	boolean okFileFormat(String fileFormat) {
		if (lowerCaseAllowedFileFormatsSet == null) {
			return true;
		}
		if (fileFormat == null) {
			return false;
		}
		
		return lowerCaseAllowedFileFormatsSet.contains(fileFormat.toLowerCase());
	}

	String getFileFormat(LocalDocument doc) {
		Object fileFormatObject = doc.getContentField(fileFormatField);

		return fileFormatHelper(fileFormatObject, 0);
	}

	private String fileFormatHelper(Object fileFormatObject, int depth) {
		if (fileFormatObject == null || depth > 1)
			return null;

		if (fileFormatObject instanceof List<?>) {
			if (((List<?>) fileFormatObject).size() > 0) {
				return fileFormatHelper(((List<?>) fileFormatObject).get(0), depth + 1);
			} else {
				return null;
			}
		}
		if (fileFormatObject instanceof String) {
			return (String) fileFormatObject;
		}

		logger.debug("Failed to parse fileFormat");
		return null;
	}

	long getFileSize(LocalDocument doc) {
		Object fileSizeObject = doc.getContentField(fileSizeField);

		return fileSizeHelper(fileSizeObject, 0);
	}

	private long fileSizeHelper(Object fileSizeObject, int depth) {
		if (fileSizeObject == null || depth > 1)
			return -1;

		if (fileSizeObject instanceof List<?>) {
			if (((List<?>) fileSizeObject).size() > 0) {
				return fileSizeHelper(((List<?>) fileSizeObject).get(0), depth + 1);
			} else {
				return -1;
			}
		}
		if (fileSizeObject instanceof String) {
			return Long.parseLong((String) fileSizeObject);
		}
		if (fileSizeObject instanceof Number) {
			return ((Number) fileSizeObject).longValue();
		}

		logger.warn("File size could not be parsed");
		return -1;
	}

	boolean okFileSize(long size) {
		if (maxSizeInBytes < 0)
			return true;

		return (size <= maxSizeInBytes);
	}

	private void enrichDocumentWithFileContents(LocalDocument doc, InputStream stream) throws IOException,
			SAXException, TikaException {
		Metadata metadata = new Metadata();
		Parser parser = new AutoDetectParser();
		ParseContext parseContext = new ParseContext();
		parseContext.set(Parser.class, parser);

		StringWriter textData = new StringWriter();
		parser.parse(stream, new BodyContentHandler(textData), metadata, parseContext);

		addTextToDocument(doc, textData);
		addMetadataToDocument(doc, metadata);

	}

	void addTextToDocument(LocalDocument doc, StringWriter textData) {
		doc.putContentField(contentField, textData.toString());
	}

	void addMetadataToDocument(LocalDocument doc, Metadata metadata) {
		for (String name : metadata.names()) {
			if (metadata.getValues(name).length > 1) {
				doc.putContentField(metadataPrefix + name, Arrays.asList(metadata.getValues(name)));
			} else {
				doc.putContentField(metadataPrefix + name, metadata.get(name));
			}
		}
	}

	InputStream getStreamFromUrl(String stringUrl) throws IOException {
		URL url = new URL(stringUrl);
		InputStream in = url.openStream();
		return in;
	}

	@Override
	public void init() throws RequiredArgumentMissingException {
		if (metadataPrefix == null) {
			throw new RequiredArgumentMissingException("Missing required configuration: metadataPrefix");
		}
		if (contentField == null) {
			throw new RequiredArgumentMissingException("Missing required configuration: contentField");
		}
		if (urlField == null) {
			throw new RequiredArgumentMissingException("Missing required configuration: urlField");
		}
		if (fileSizeField == null && maxSizeInBytes > 0) {
			throw new RequiredArgumentMissingException(
					"Missing required configuration: fileSizeField - FileSizeField must be set when maxSizeInBytes is set");
		}
		if (allowedFileFormats != null && fileFormatField == null) {
				throw new RequiredArgumentMissingException(
					"Missing required configuration: fileFormatField - fileFormatField must be set when allowedFileFormats is set");
		}
	
		setLowerCaseAllowedFileFormats();
	}

	void setLowerCaseAllowedFileFormats() {
		if (allowedFileFormats == null || allowedFileFormats.size() <= 0)
			return;
		lowerCaseAllowedFileFormatsSet = new HashSet<String>();
		for (String allowedFileFormat : allowedFileFormats) {
			lowerCaseAllowedFileFormatsSet.add(allowedFileFormat.toLowerCase());
		}
	}

	/**
	 * Fetches a url from the field specified by the urlField.
	 * 
	 * If the field is a List<String> returns the FIRST String. The rest will be
	 * ignored.
	 * 
	 * @param doc
	 *            The document to get the url from
	 * @return The String in if it is a String, the first String if it is a
	 *         List<String> or null otherwise
	 */
	String getUrl(LocalDocument doc) {
		Object urlObject = doc.getContentField(urlField);

		if (urlObject == null) {
			return null;
		}

		if (urlObject instanceof String) {
			return (String) urlObject;
		} else if (urlObject instanceof List<?>) {
			List<?> urlList = (List<?>) urlObject;
			if (!urlList.isEmpty()) {
				if (urlList.get(0) instanceof String) {
					return (String) urlList.get(0);
				} else {
					logger.warn("List in " + urlField + " did not contain Strings. Skipping");
					return null;
				}
			} else {
				logger.warn("List was empty. Skipping");
			}
		} else {
			logger.warn(urlField + " did not contain String nor List. Skipping");
		}

		return null;
	}

	public void setUrlField(String urlField) {
		this.urlField = urlField;
	}

	public void setContentField(String contentField) {
		this.contentField = contentField;
	}

	public String getContentField() {
		return contentField;
	}

	public void setMetadatPrefix(String prefix) {
		this.metadataPrefix = prefix;
	}

	public String getMetadataPrefix() {
		return metadataPrefix;
	}

	public long getMaxSizeInBytes() {
		return maxSizeInBytes;
	}

	public void setMaxSizeInBytes(long size) {
		this.maxSizeInBytes = size;
	}

	public String getFileSizeField() {
		return fileSizeField;
	}

	public void setFileSizeField(String fileSizeField) {
		this.fileSizeField = fileSizeField;
	}

	public String getFileFormatField() {
		return fileFormatField;
	}

	public void setFileFormatField(String fileFormatField) {
		this.fileFormatField = fileFormatField;
	}

	public List<String> getAllowedFileFormats() {
		return allowedFileFormats;
	}

	public void setAllowedFileFormats(List<String> allowedFileFormats) throws RequiredArgumentMissingException {
		this.allowedFileFormats = allowedFileFormats;
	}
}
