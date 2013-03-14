package com.findwise.hydra.input.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.Logger;
import com.findwise.hydra.input.HttpInputServer;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

@Deprecated
@Stage
public class SolrInputStage extends AbstractInputStage implements HttpRequestHandler {

	private HttpInputServer server;
	
	@Parameter
	private int port = 8051;
	
	@Override
	public void init() throws RequiredArgumentMissingException {
		
		if (port != 0) {
			Logger.info("Starting Solr Input Server on port: " + port);
			server = new HttpInputServer(port, this);
		} else {
			Logger.info("Starting Solr Input Server on default port: " + HttpInputServer.DEFAULT_LISTEN_PORT);
			server = new HttpInputServer(HttpInputServer.DEFAULT_LISTEN_PORT, this);
		}

		server.init();
	}	
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
		Logger.debug("Parsing incoming request");
		String requestUri = request.getRequestLine().getUri();

		if (!requestUri.matches("/update.*")) {
			response.setEntity(getAddSuccessAnswer());
			response.setStatusCode(200);
			return;
		}
		
		HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
		String content = IOUtils.toString(requestEntity.getContent());
		handleBody(content);

		response.setEntity(getAddSuccessAnswer());
		response.setStatusCode(200);
	}

	private HttpEntity getAddSuccessAnswer() throws IOException {
		NamedList<Object> named = new NamedList<Object>();

		named.add("response",
				  "<lst name=\"responseHeader\"><int name=\"status\">0</int><int name=\"QTime\">0</int></lst>");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		(new JavaBinCodec()).marshal(named, out);
		BasicHttpEntity entity = new BasicHttpEntity();
		entity.setContent(new ByteArrayInputStream(out.toByteArray()));
		return entity;
	}

	protected void handleBody(String input) throws  IOException {		
		DOMParser parser = new DOMParser();
		try {
			parser.parse(new InputSource(new java.io.StringReader(input)));
		} catch (SAXException e) {
			throw new IOException(e);
		}
		
		Document doc = parser.getDocument();
		doc.getDocumentElement().normalize();
		
		List<LocalDocument> documents = new ArrayList<LocalDocument>();
		if (isAddNode(doc.getDocumentElement())) {
			documents = createAddDocuments(doc.getDocumentElement());
		} else if(isDeleteNode(doc.getDocumentElement())) {
			documents = createDeleteDocuments(doc.getDocumentElement());
		}
		discardOldDocuments(documents);
		saveDocumentsToPipeline(documents);
	}
		
	private boolean isAddNode(Node node) {
		return node.getNodeName().equalsIgnoreCase("add"); 
	}

	private boolean isDeleteNode(Node node) {
		return node.getNodeName().equalsIgnoreCase("delete"); 
	}
	
	private List<LocalDocument> createAddDocuments(Node addNode) {
		List<LocalDocument> documents = new ArrayList<LocalDocument>();
		NodeList children = addNode.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (isDocNode(child)) {
				LocalDocument doc2Add = createAddDocument(child);
				documents.add(doc2Add);
			}
		}
		return documents;
	}
	
	private List<LocalDocument> createDeleteDocuments(Node deleteNode) {
		List<LocalDocument> documents = new ArrayList<LocalDocument>();
		NodeList children = deleteNode.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (isIdNode(child)) {
				LocalDocument doc2Add = createDeleteDocument(child.getTextContent());
				documents.add(doc2Add);
			}
		}
		return documents;
	}
	
	private void discardOldDocuments(List<LocalDocument> documents) throws IOException {
		for(LocalDocument document : documents) {
			try {
				discardOld(document);
			} catch (RequiredArgumentMissingException e) {
				throw new IOException(e);
			}
		}
	}
	
	private void saveDocumentsToPipeline(List<LocalDocument> documents) throws IOException {
		for(LocalDocument document : documents) {
			try {
				getRemotePipeline().saveFull(document);
			} catch (JsonException e) {
				throw new IOException(e);
			}
		}
	}
	
	private boolean isDocNode(Node node) {
		return node.getNodeName().equalsIgnoreCase("doc"); 
	}
	
	private boolean isIdNode(Node node) {
		return node.getNodeName().equalsIgnoreCase("id"); 
	}
	
	private LocalDocument createAddDocument(Node docNode) {
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		NodeList children = docNode.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (isFieldNode(child)) {
				doc.putContentField(child.getAttributes().getNamedItem("name").getTextContent(), child.getTextContent());
			}
		}
		return doc;
	}
	
	private LocalDocument createDeleteDocument(String id) {
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.DELETE);
		doc.putContentField("id", id);
		return doc;
	}
	
	private boolean isFieldNode(Node node) {
		return node.getNodeName().equalsIgnoreCase("field"); 
	}
}
