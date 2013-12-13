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
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.tools.HttpConnection;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class RemotePipeline {
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
	public static final String DEFAULT_HOST = "127.0.0.1";

	private boolean performanceLogging = false;

	private HttpConnection core;

	private String getUrl;
	private String writeUrl;
	private String processedUrl;
	private String failedUrl;
	private String pendingUrl;
	private String discardedUrl;
	private String propertyUrl;
	private String fileUrl;

	private String stageName;

	private LocalDocument currentDocument;

	/**
	 * Calls RemotePipeline(String, int, String) with default values for
	 * hostName (RemotePipeline.DEFAULT_HOST) and port (RemotePipeline.DEFAULT_PORT).
	 *
	 * @param stageName
	 */
	public RemotePipeline(String stageName) {
		this(DEFAULT_HOST, DEFAULT_PORT, stageName);
	}

	public RemotePipeline(String hostName, int port, String stageName) {
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
			try {
				startJson = System.currentTimeMillis();
				ld = new LocalDocument(s);
			} catch (JsonException e) {
				throw new IOException(e);
			}
			internalLogger.debug("Received document with ID " + ld.getID());
			currentDocument = ld;
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

	private static void logUnexpected(HttpResponse response) throws IOException {
		internalLogger.error("Node gave an unexpected response: " + response.getStatusLine());
		internalLogger.error("Message: " + EntityUtils.toString(response.getEntity()));
	}

	/**
	 * Writes all outstanding updates to the last document fetched from the pipeline.
	 */
	public boolean saveCurrentDocument() throws IOException, JsonException {
		if (currentDocument == null) {
			internalLogger.error("There is no document to write.");
			return false;
		}
		boolean x = save(currentDocument);
		if (x) {
			currentDocument = null;
		}

		return x;
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

	public Map<String, Object> getProperties() throws IOException {
		HttpResponse response = core.get(propertyUrl);

		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			Map<String, Object> map;
			try {
				map = SerializationUtils.fromJson(EntityUtils.toString(response.getEntity()));
			} catch (JsonException e) {
				throw new IOException(e);
			}
			internalLogger.debug("Successfully retrieved propertyMap with " + map.size() + " entries");
			return map;
		} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
			internalLogger.debug("No document found matching query");
			EntityUtils.consume(response.getEntity());
			return null;
		} else {
			logUnexpected(response);
			return null;
		}
	}

	private String getFileUrl(DocumentFile<Local> df) throws UnsupportedEncodingException {
		return getFileUrl(df.getFileName(), df.getDocumentId());
	}

	private String getFileUrl(String fileName, DocumentID<Local> docid) throws UnsupportedEncodingException {
		return fileUrl + "&" + RemotePipeline.FILENAME_PARAM + "=" + fileName + "&" + RemotePipeline.DOCID_PARAM + "=" + URLEncoder.encode(docid.toJSON(), "UTF-8");
	}

	public DocumentFile<Local> getFile(String fileName, DocumentID<Local> docid) throws IOException {
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
	}

	public boolean saveFile(DocumentFile<Local> df) throws IOException {
		HttpResponse response = core.post(getFileUrl(df), SerializationUtils.toJson(df));
		int code = response.getStatusLine().getStatusCode();
		if (code == HttpStatus.SC_OK || code == HttpStatus.SC_NO_CONTENT) {
			EntityUtils.consume(response.getEntity());
			return true;
		} else {
			logUnexpected(response);
			return false;
		}
	}

	public boolean deleteFile(String fileName, DocumentID<Local> docid) throws IOException {
		HttpResponse response = core.delete(getFileUrl(fileName, docid));

		if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			EntityUtils.consume(response.getEntity());
			return true;
		} else {
			logUnexpected(response);
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getFileNames(DocumentID<?> docid) throws IOException {
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
	}

	public List<DocumentFile<Local>> getFiles(DocumentID<Local> docid) throws IOException {
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

	public void setPerformanceLogging(boolean performanceLogging) {
		this.performanceLogging = performanceLogging;
	}

	public boolean isPerformanceLogging() {
		return performanceLogging;
	}
}
