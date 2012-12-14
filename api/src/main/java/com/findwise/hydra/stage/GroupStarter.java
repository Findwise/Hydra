package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.tools.HttpConnection;

/**
 * 
 * @author joel.westberg
 *
 */
public class GroupStarter {
	public static final String GET_STAGES_URL = "getStages";
	public static final String GROUP_PARAM = "group";
	
	public static void main(String[] args) {
		if (args.length < 1) {
			Logger.error("No group name found", new RequiredArgumentMissingException("No group name specified"));
			System.exit(1);
		} 
		String host;
		String port;
		String groupName = args[0];
		String logging;
		if(args.length == 1) {
			host = "localhost";
			port = "12001";
			logging = "false";
		}
		else {
			host = args[1];
			port = args[2];
			logging = args[3];
		}
		
		List<String> stages;
		try {
			stages = getStages(host, Integer.parseInt(port), groupName);
		} catch (IOException e) {
			Logger.error("Unable to get stages for this group", e);
			return;
		}
		
		for(String stage : stages) {
			Logger.debug("Attempting to start stage: "+stage);
			AbstractStage.main(new String[]{stage, host, port, logging});
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<String> getStages(String host, int port, String group) throws IOException {
		HttpConnection connection = new HttpConnection(host, port);
		HttpResponse response = connection.get("/"+GET_STAGES_URL+"?"+GROUP_PARAM+"="+group);
		
		if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			Logger.error("Unable to get list stages in the group");
			return new ArrayList<String>();
		}
		
		try {
			return (List<String>) SerializationUtils.toObject(EntityUtils.toString(response.getEntity()));
		} catch (JsonException e) {
			Logger.error("Unable to deserialize list of stages", e);
			return new ArrayList<String>();
		}
	}
}
