package com.findwise.hydra.local;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentFileRepository;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.AbstractProcessStageMapper;
import com.findwise.hydra.stage.InitFailedException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.tools.HttpConnection;

public class RemotePipeline implements DocumentFileRepository {
	private static final Logger internalLogger = LoggerFactory.getLogger("internal");
	private static final Logger logger = LoggerFactory.getLogger(RemotePipeline.class);

	public static final String GET_DOCUMENT_URL = "getDocument";
	public static final String WRITE_DOCUMENT_URL = "writeDocument";
	public static final String RELEASE_DOCUMENT_URL = "releaseDocument";
	public static final String PROCESSED_DOCUMENT_URL = "processedDocument";
	public static final String PENDING_DOCUMENT_URL = "pendingDocument";
	public static final String DISCARDED_DOCUMENT_URL = "discardedDocument";
	public static final String GET_PROPERTIES_URL = "getProperties";
	public static final String FAILED_DOCUMENT_URL = "failedDocument";
	public static final String FILE_URL = "documentFile";

	public static final String STAGE_PARAM = "stage";
	public static final String NORELEASE_PARAM = "norelease";
	public static final String PARTIAL_PARAM = "partial";
	public static final String DOCID_PARAM = "docid";
	public static final String FILENAME_PARAM = "filename";

	public static final int DEFAULT_PORT = 12001;
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_LOG_PORT = 12002;

	private final boolean performanceLogging;

	private final HttpConnection core;

	private final String getUrl;
	private final String writeUrl;
	private final String processedUrl;
	private final String failedUrl;
	private final String pendingUrl;
	private final String discardedUrl;
	private final String propertyUrl;
	private final String fileUrl;

	private final String stageName;

	/**
	 * Calls RemotePipeline(String, int, String) with default values for
	 * hostName (RemotePipeline.DEFAULT_HOST) and port (RemotePipeline.DEFAULT_PORT).
	 *
	 * @param stageName
	 */
	public RemotePipeline(String stageName) {
		this(DEFAULT_HOST, DEFAULT_PORT, stageName, false);
	}

	public RemotePipeline(String hostName, int port, String stageName) {
		this(hostName, port, stageName, false);
	}

	public RemotePipeline(String hostName, int port, String stageName, boolean performanceLogging) {
		this.stageName = stageName;
		getUrl = "/" + GET_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		writeUrl = "/" + WRITE_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		processedUrl = "/" + PROCESSED_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		failedUrl = "/" + FAILED_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		pendingUrl = "/" + PENDING_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		discardedUrl = "/" + DISCARDED_DOCUMENT_URL + "?" + STAGE_PARAM + "=" + stageName;
		propertyUrl = "/" + GET_PROPERTIES_URL + "?" + STAGE_PARAM + "=" + stageName;
		fileUrl = "/" + FILE_URL + "?" + STAGE_PARAM + "=" + stageName;

		core = new HttpConnection(hostName, port);
		this.performanceLogging = performanceLogging;
	}

	/**
	 * Non-recurring, use this in all known cases except for in an output node.
	 * <p/>
	 * The fetched document will be tagged with the name of the stage which is
	 * used to execute getDocument.
	 */
	public LocalDocument getDocument(LocalQuery query) throws IOException {
		HttpResponse response;
		long start = System.currentTimeMillis();
		response = core.post(getUrl, query.toJson());

		long startSerialize = System.currentTimeMillis();
		long startJson = 0L;
		LocalDocument ld = null;
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			String s = EntityUtils.toString(response.getEntity());
			startJson = System.currentTimeMillis();
			ld = buildDocument(s);
			internalLogger.debug("Received document with ID " + ld.getID());
		} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
			internalLogger.debug("No document found matching query");
			EntityUtils.consume(response.getEntity());
		} else {
			logUnexpected(response);
		}
		if (isPerformanceLogging()) {
			long end = System.currentTimeMillis();
			Object docId = ld != null ? ld.getID() : null;
			logger.info(String.format("type=performance event=query stage_name=%s doc_id=\"%s\" start=%d fetch=%d entitystring=%d serialize=%d end=%d total=%d", stageName, docId, start, startSerialize - start, startJson - startSerialize, end - startJson, end, end - start));
		}
		return ld;
	}

	private LocalDocument buildDocument(String s) throws IOException {
		LocalDocument ld;
		try {
			ld = new LocalDocument(s);
		} catch (JsonException e) {
			// TODO: Why IOException here?
			throw new IOException(e);
		}
		ld.setDocumentFileRepository(this);
		return ld;
	}

	private static void logUnexpected(HttpResponse response) throws IOException {
		internalLogger.error("Node gave an unexpected response: " + response.getStatusLine());
		internalLogger.error("Message: " + EntityUtils.toString(response.getEntity()));
	}

	/**
	 * Writes an entire document to the pipeline. Use is discouraged, try using save(..) whenever possible.
	 */
	public boolean saveFull(LocalDocument d) throws IOException, JsonException {
		boolean res = save(d, false);
		if (res) {
			d.markSynced();
		}
		return res;
	}

	/**
	 * Writes all outstanding updates to the document since it was initialized.
	 */
	public boolean save(LocalDocument d) throws IOException, JsonException {
		boolean res = save(d, true);
		if (res) {
			d.markSynced();
		}
		return res;
	}

	private boolean save(LocalDocument d, boolean partialUpdate) throws IOException, JsonException {
		boolean hasId = d.getID() != null;
		String s;
		long start = System.currentTimeMillis();
		if (partialUpdate) {
			s = d.modifiedFieldsToJson();
		} else {
			s = d.toJson();
		}
		long startPost = System.currentTimeMillis();
		HttpResponse response = core.post(getWriteUrl(partialUpdate), s);
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			if (!hasId) {
				LocalDocument updated = new LocalDocument(EntityUtils.toString(response.getEntity()));
				d.putAll(updated);
			} else {
				EntityUtils.consume(response.getEntity());
			}
			if (isPerformanceLogging()) {
				long end = System.currentTimeMillis();
				DocumentID<Local> docId = d.getID();
				logger.info(String.format("type=performance event=update stage_name=%s doc_id=\"%s\" start=%d serialize=%d post=%d end=%d total=%d", stageName, docId, start, startPost - start, end - startPost, end, end - start));
			}
			return true;
		}

		logUnexpected(response);
		return false;
	}

	public boolean markPending(LocalDocument d) throws IOException {
		HttpResponse response = core.post(pendingUrl, d.contentFieldsToJson(null));
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			EntityUtils.consume(response.getEntity());

			return true;
		}

		logUnexpected(response);

		return false;
	}

	public boolean markFailed(LocalDocument d) throws IOException {
		HttpResponse response = core.post(failedUrl, d.modifiedFieldsToJson());
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			EntityUtils.consume(response.getEntity());

			return true;
		}

		logUnexpected(response);

		return false;
	}

	public boolean markFailed(LocalDocument d, Throwable t) throws IOException {
		d.addError(stageName, t);
		return markFailed(d);
	}

	public boolean markProcessed(LocalDocument d) throws IOException {
		HttpResponse response = core.post(processedUrl, d.modifiedFieldsToJson());
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			EntityUtils.consume(response.getEntity());

			return true;
		}

		logUnexpected(response);

		return false;
	}

	public boolean markDiscarded(LocalDocument d) throws IOException {
		HttpResponse response = core.post(discardedUrl, d.modifiedFieldsToJson());
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			EntityUtils.consume(response.getEntity());

			return true;
		}

		logUnexpected(response);

		return false;
	}

	private String getWriteUrl(boolean partialUpdate) {
		String s = writeUrl;
		s += "&" + NORELEASE_PARAM + "=0";
		if (partialUpdate) {
			s += "&" + PARTIAL_PARAM + "=1";
		} else {
			s += "&" + PARTIAL_PARAM + "=0";
		}
		return s;
	}

	public AbstractProcessStage getStageInstance() throws IOException, IllegalAccessException, InitFailedException, InstantiationException, JsonException, RequiredArgumentMissingException, ClassNotFoundException {
		String jsonString = getStagePropertiesJsonString();
		return AbstractProcessStageMapper.fromJsonString(jsonString);
	}

	private String getStagePropertiesJsonString() throws IOException {
		HttpResponse response = core.get(propertyUrl);

		String jsonString;
		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			internalLogger.debug("Successfully retrieved propertyMap");
			jsonString = EntityUtils.toString(response.getEntity());
		} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
			internalLogger.debug("No document found matching query");
			EntityUtils.consume(response.getEntity());
			throw new RuntimeException("No stage properties found for " + stageName);
		} else {
			logUnexpected(response);
			throw new RuntimeException("Unexpected error while fetching stage properties for " + stageName);
		}
		return jsonString;
	}

	private String getFileUrl(DocumentFile<Local> df) throws UnsupportedEncodingException {
		return getFileUrl(df.getFileName(), df.getDocumentId());
	}

	private String getFileUrl(String fileName, DocumentID<Local> docid) throws UnsupportedEncodingException {
		return fileUrl + "&" + RemotePipeline.FILENAME_PARAM + "=" + fileName + "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(docid.toJSON(), "UTF-8");
	}

	public DocumentFile<Local> getFile(String fileName, DocumentID<Local> docid) {
		try {
			HttpResponse response = core.get(getFileUrl(fileName, docid));

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				Object o;
				try {
					o = SerializationUtils.toObject(EntityUtils.toString(response.getEntity()));
				} catch (JsonException e) {
					throw new IOException(e);
				}
				if (!(o instanceof Map)) {
					return null;
				}

				@SuppressWarnings("unchecked")
				Map<String, Object> map = (Map<String, Object>) o;
				Date d = (Date) map.get("uploadDate");
				String encoding = (String) map.get("encoding");
				String mimetype = (String) map.get("mimetype");
				String savedByStage = (String) map.get("savedByStage");
				InputStream is;
				if (encoding == null) {
					is = new ByteArrayInputStream(Base64.decodeBase64(((String) map.get("stream")).getBytes("UTF-8")));
				} else {
					is = new ByteArrayInputStream(Base64.decodeBase64(((String) map.get("stream")).getBytes(encoding)));
				}

				DocumentFile<Local> df = new DocumentFile<Local>(docid, fileName, is, savedByStage, d);
				df.setEncoding(encoding);
				df.setMimetype(mimetype);

				return df;
			} else {
				logUnexpected(response);
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean saveFile(DocumentFile<Local> df) {
		try {
			HttpResponse response = core.post(getFileUrl(df), SerializationUtils.toJson(df));
			int code = response.getStatusLine().getStatusCode();
			if (code == HttpStatus.SC_OK || code == HttpStatus.SC_NO_CONTENT) {
				EntityUtils.consume(response.getEntity());
				return true;
			} else {
				logUnexpected(response);
				return false;
			}
		} catch(IOException e) {
			// TODO: Something else here?
			throw new RuntimeException(e);
		}
	}

	public boolean deleteFile(String fileName, DocumentID<Local> docid) {
		try {
			HttpResponse response = core.delete(getFileUrl(fileName, docid));

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				EntityUtils.consume(response.getEntity());
				return true;
			} else {
				logUnexpected(response);
				return false;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getFileNames(DocumentID<?> docid) {
		try {
			HttpResponse response = core.get(fileUrl + "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(docid.toJSON(), "UTF-8"));

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				try {
					return (List<String>) SerializationUtils.toObject(EntityUtils.toString(response.getEntity()));
				} catch (JsonException e) {
					throw new IOException(e);
				}
			} else {
				logUnexpected(response);
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<DocumentFile<Local>> getFiles(DocumentID<Local> docid) {
		List<String> fileNames = getFileNames(docid);
		List<DocumentFile<Local>> files = new ArrayList<DocumentFile<Local>>();
		for (String fileName : fileNames) {
			files.add(getFile(fileName, docid));
		}
		return files;
	}

	public String getStageName() {
		return stageName;
	}

	public boolean isPerformanceLogging() {
		return performanceLogging;
	}
}
