package com.findwise.hydra.admin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.findwise.hydra.StoredPipeline;
import com.findwise.hydra.StoredStage;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.Guice;

/**
 * This class WILL BE REMOVED before 0.2.
 * @author joel.westberg
 */
@Deprecated
public class ConfigurationWriter {
	
	
	public static void main(String[] args) throws IOException {
		String database = "blahonga";
		
		if (args.length > 3) {
			database = args[3];
		}
		
		MongoConnector mdc = Guice.createInjector(new Module(database))
				.getInstance(MongoConnector.class);
		
		mdc.connect();

		if (args[0].equals("add")) {
			System.out.println("Adding " + args[1]);
			
			StoredPipeline c = new StoredPipeline(database);
			// c.readStagesFromDisk();
			StoredStage s = new StoredStage(args[1], mdc.getPipelineReader().getFile(args[2]));
			c.addStage(s);
			//s.setJar(new File(args[2]), null);
			Map<String, Object> props = readPropertiesFile(args[1]);
			/*
			 * props.put("stageClass",
			 * "com.findwise.hydra.output.idol.IDOLOutputStage");
			 * props.put("idolHost", "127.0.0.1"); props.put("idolIndexPort",
			 * "9071"); props.put("fieldExists", "_idolAction");
			 */
			s.setProperties(props);
			mdc.getPipelineWriter().write(c);
		} else {
			System.out.println("No command given. Syntax should be:");
			System.out
					.println("java -jar hydra-admin-jar-with-dependencies.jar add <stage name> <jar file>");
			System.out.println("or:");
			System.out
					.println("java -jar hydra-admin-jar-with-dependencies.jar delete");
		}
	}

	private static Map<String, Object> readPropertiesFile(String stageName) {
		String json="";
		try {
			json = readFileAsString(stageName+".properties");
		} catch (IOException e) {
			System.err.println("Property file "+stageName+".properties could not be read");
			System.exit(-1);
		}
		try {
			return SerializationUtils.fromJson(json);
		} catch (JsonException e) {
			System.err.println("Property file "+stageName+".properties is not well formed json");
			System.exit(-1);
		}
		return null;
	}

	private static String readFileAsString(String filePath)
			throws java.io.IOException {
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
			buf = new char[1024];
		}
		reader.close();
		return fileData.toString();
	}

	// public static Map<String, Object> readPropertiesFile(String stageName) {
	//
	// Map<String, Object> properties = new HashMap<String, Object>();
	//
	// try {
	// BufferedReader in = new BufferedReader(new FileReader(stageName
	// + ".properties"));
	// String str;
	// if (in != null) {
	// while ((str = in.readLine()) != null) {
	// String[] vals = str.split("=", 2);
	// if (vals.length != 2) {
	// System.err.println("Properties file error on line: " + str
	// +"\nLine does not contain =");
	// continue;
	// }
	// addProperty(properties, vals[0], vals[1]);
	// }
	//
	// in.close();
	// }
	// } catch (IOException iOException) {
	//
	// }
	// return properties;
	// }
	//
	// @SuppressWarnings("unchecked")
	// private static void addProperty(Map<String, Object> properties, String
	// key,
	// String value) {
	// List<String> values = (List<String>) properties.get(key);
	// if (values == null) {
	// values = new ArrayList<String>();
	// properties.put(key, values);
	// }
	// values.add(value);
	// }
}