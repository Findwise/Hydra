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
import org.slf4j.LoggerFactory;

/**
 * 
 * @author joel.westberg
 *
 */
public class GroupStarter {
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(GroupStarter.class);
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
		if(args.length == 1) {
			host = "localhost";
			port = "12001";
			logging = "false";
			logPort = "12002";
		}
		else {
			host = args[1];
			port = args[2];
			logging = args[3];
			logPort = args[4];
		}

		Logging.setup(host, Integer.parseInt(logPort));

		List<String> stages;
		try {
			stages = getStages(host, Integer.parseInt(port), groupName);
		} catch (IOException e) {
			logger.error("Unable to get stages for this group", e);
			return;
		}
		
		for(String stage : stages) {
			logger.debug("Attempting to start stage: "+stage);
			AbstractStage.main(new String[]{stage, host, port, logging, logPort});
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
