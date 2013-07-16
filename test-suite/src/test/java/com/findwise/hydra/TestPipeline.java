package com.findwise.hydra;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.findwise.hydra.mongodb.MongoQuery;
import com.findwise.hydra.stage.AbstractStage;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class TestPipeline {

	private static final int HYDRA_LOGGING_PORT = 12002;
	private static final boolean HYDRA_PERFORMANCE_LOGGING_ENABLED = false;
	private static final int HYRDA_REST_PORT = 12001;
	private static final String HYDRA_HOST = "localhost";
	
	private static final int MONGO_PORT = 27017;
	private static final String MONGO_HOST = "localhost";
	
	private static final int STEPS = 500;
	private static final int TIMEOUT = 20000;

	private static MongodExecutable mongodExecutable;
	private static MongoDocumentIO io;
	
	private static Map<String, String> stageConfigurationMap = new HashMap<String, String>();
	private static Map<String, Object> inputContents = new HashMap<String, Object>();
	private static Map<String, Object> expectedOutputContents = new HashMap<String, Object>();
	private static MongoQuery query = new MongoQuery();
	
	/**
	 * Set your query to see if processing is done here
	 */
	private static void setMongoQuery() {
		query.requireTouchedByStage("staticField");
	}
	
	/**
	 * Add your stages here
	 */
	private static void setupStages() {
		stageConfigurationMap.put("staticField", "src/test/resources/staticField.properties");		
	}

	/**
	 * Add you input and expected output here
	 */
	private static void setupDocuments() {
		inputContents.put("title", "my title");
		inputContents.put("text", "my text");

		expectedOutputContents.put("source", "my source");
		expectedOutputContents.put("title", "my title");
		expectedOutputContents.put("text", "my text");
	}

	
	@Test
	public void testPipeline() throws IllegalArgumentException,
			IllegalAccessException, JsonException, IOException,
			InterruptedException, TimeoutException {
		
		addDocument(inputContents);
		MongoDocument document = blockingGetProcessedDocument();
		assertDocumentContentsEquals(expectedOutputContents, document.getContentMap());
		
	}

	
	/*
	  --------------------- Helpers and setup below this line --------------------
	*/
	
	
	@BeforeClass
	public static void startUp() throws IOException {
		setupStages();
		setupDocuments();
		setMongoQuery();
		
		startMongo();
		startHydra();
		
		setupMongoDocumentIO();

		startAllStages();
	}

	private static void setupMongoDocumentIO() throws UnknownHostException {
		Mongo mongo = new Mongo(MONGO_HOST, MONGO_PORT);
		DB db = mongo.getDB("pipeline");
		
		io = new MongoDocumentIO(db, new WriteConcern(), 0, 0, null, null);
	}

	private static void startHydra() {
		Main.main(new String[0]);
	}

	private static void startMongo() throws UnknownHostException, IOException {
		MongodConfig mongodConfig = new MongodConfig(Version.Main.PRODUCTION,
				MONGO_PORT, Network.localhostIsIPv6());

		MongodStarter runtime = MongodStarter.getDefaultInstance();

		mongodExecutable = runtime.prepare(mongodConfig);
		mongodExecutable.start();
	}	


	private static void startStage(String stageName, String config)
			throws UnknownHostException {
		AbstractStage.main(new String[] { stageName, HYDRA_HOST, HYRDA_REST_PORT + "", HYDRA_PERFORMANCE_LOGGING_ENABLED + "", HYDRA_LOGGING_PORT + "", config });
	}	

	private static void startAllStages() throws IOException, UnknownHostException {
		for (Entry<String, String> entry : stageConfigurationMap.entrySet()) {
			File f = new File(entry.getValue());
			String config = IOUtils.toString(f.toURI());

			startStage(entry.getKey(), config);
			
		}
	}
	
	private void assertDocumentContentsEquals(
			Map<String, Object> expectedOutputContents,
			Map<String, Object> contentMap) {
		Assert.assertEquals(expectedOutputContents.size(), contentMap.size());
		
		for (Entry<String, Object> entry : expectedOutputContents.entrySet()) {
			Assert.assertEquals(entry.getValue(), contentMap.get(entry.getKey()));
		}
	}

	private MongoDocument blockingGetProcessedDocument() throws JsonException, InterruptedException, TimeoutException {
		for (int i = 0; i < TIMEOUT; i+=STEPS) {
			MongoDocument document = io.getDocument(query);
			
			if (document != null) {
				return document;
			}
			Thread.sleep(STEPS);
		}
		
		throw new TimeoutException("Failed to get document within configured time limit");
	}

	private void addDocument(Map<String, Object> contents) throws IOException, JsonException {
		LocalDocument doc = new LocalDocument();
		for (Entry<String, Object> entry : contents.entrySet()) {
			doc.putContentField(entry.getKey(), entry.getValue());
		}
		
		io.insert(new MongoDocument(doc.toJson()));
	}
}
