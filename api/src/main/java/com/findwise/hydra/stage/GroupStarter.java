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
		if (args.length < 1) {
			logger.error("No group name found", new RequiredArgumentMissingException("No group name specified"));
			System.exit(1);
		} 
		String host;
		String port;
		String groupName = args[0];
		String logging;
		String logPort;
		
		host = (args.length>1) ? args[1] : "localhost";
		port = (args.length>2) ? args[2] : "12001";
		logging = (args.length>3) ? args[3] : "false";
		logPort = (args.length>4) ? args[4] : "12002";

		try {
			Logging.setup(host, Integer.parseInt(logPort));
		} catch (UnknownHostException e) {
			logger.error("Unable to connect to remote logging host on "+host+":"+logPort, e);
			return;
		}

		List<String> stages;
		try {
			stages = getStages(host, Integer.parseInt(port), groupName);
		} catch (IOException e) {
			Logging.addConsoleAppender();
			logger.error("Unable to get stages for the group '"+groupName+"'", e);
			return;
		}

		try {
			for (String stage : stages) {
				logger.debug("Attempting to start stage: " + stage);
				AbstractStage.main(new String[] { stage, host, port, logging, logPort });
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
