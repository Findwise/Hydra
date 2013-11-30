package com.findwise.hydra.stage;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.Logging;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.tools.HttpConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a StageGroup in a new JVM by fetching stage names and setting up logging.
 * This class delegates the actual startup procedure to {@link AbstractStage}.
 *
 * @author joel.westberg
 *
 */
public class GroupStarter {
    public static final Logger logger = LoggerFactory.getLogger(GroupStarter.class);
	public static final String GET_STAGES_URL = "getStages";
	public static final String GROUP_PARAM = "group";
	
	public static void main(String[] args) throws UnknownHostException {
		StageCommandLineArguments cmdLineArgs = null;
		try {
			cmdLineArgs = StageCommandLineArguments.parse(args);
		} catch (Exception e) {
			logger.error("Error parsing arguments", e);
			System.exit(1);
		}

		String groupName = cmdLineArgs.getStageGroupName();
		String host = cmdLineArgs.getHost();
		int port = cmdLineArgs.getPort();
		boolean performanceLogging = cmdLineArgs.isPerformanceLogging();
		int logPort = cmdLineArgs.getLogPort();

		try {
			Logging.setup(host, logPort);
		} catch (UnknownHostException e) {
			logger.error("Unable to connect to remote logging host on "+ host +":"+cmdLineArgs.getLogPort(), e);
			System.exit(1);
		}

		createAndStartGroup(
			groupName,
			host,
			port,
			performanceLogging
		);
	}

	private static void createAndStartGroup(String groupName, String host, int port, boolean performanceLogging) {
		List<String> stageNames;
		try {
			stageNames = getStages(host, port, groupName);
		} catch (IOException e) {
			Logging.addConsoleAppender();
			logger.error("Unable to get stages for the group '"+groupName+"'", e);
			return;
		}

		try {
			for (String stageName : stageNames) {
				logger.debug("Attempting to start stage: " + stageName);
				new StageFactory(stageName, host, port, performanceLogging, null).createAndStartStages();
			}
		} catch (Exception e) {
			Logging.addConsoleAppender();
			logger.error("An exception was thrown when starting the stages in the group", e);
		}
	}

    @SuppressWarnings("unchecked")
	public static List<String> getStages(String host, int port, String group) throws IOException {
		HttpConnection connection = new HttpConnection(host, port);
		HttpResponse response = connection.get("/"+GET_STAGES_URL+"?"+GROUP_PARAM+"="+group);
		
		if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			logger.error("Unable to get list stages in the group");
			return new ArrayList<String>();
		}
		
		try {
			return (List<String>) SerializationUtils.toObject(EntityUtils.toString(response.getEntity()));
		} catch (JsonException e) {
			logger.error("Unable to deserialize list of stages", e);
			return new ArrayList<String>();
		}
	}
}
