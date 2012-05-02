package com.findwise.hydra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.admin.Module;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.Guice;

public class StraightPipelineSetup {
	public static void main(String[] args) throws Exception {
		MongoConnector mdc = Guice.createInjector(new Module("pipeline"))
		.getInstance(MongoConnector.class);

		mdc.connect();
		
		Object outId = addFile(mdc, "hydra-out-jar-with-dependencies.jar");
		Object basicId = addFile(mdc, "basic-stages-jar-with-dependencies.jar");
		
		
		
		
		Pipeline<Stage> c = new Pipeline<Stage>();
		Stage s = getStage(c, basicId, "copyStage1", "stage.CopyStage");
		Map<String, Object> map = s.getProperties();
		map.put("map", getSingleMap("in", "out1"));
		s.setProperties(map);
		
		s = getStage(c, basicId, "copyStage2", "stage.CopyStage", "copyStage1");
		map = s.getProperties();
		map.put("map", getSingleMap("out1", "out2"));
		s.setProperties(map);
		
		s = getStage(c, basicId, "copyStage3", "stage.CopyStage", "copyStage2");
		map = s.getProperties();
		map.put("map", getSingleMap("out2", "out3"));
		s.setProperties(map);
		
		s = getStage(c, outId, "solrOutput", "output.solr.SolrOutputStage", "copyStage3");
		map = s.getProperties();
		HashMap<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("out1", "out1_s");
		fieldMap.put("out2", "out2_s");
		fieldMap.put("out3", "out3_s");
		fieldMap.put("in", "in_s");
		fieldMap.put("id", "id");
		map.put("fieldMappings", fieldMap);
		map.put("solrDeployPath", "http://127.0.0.1:8983/solr");
		s.setProperties(map);
		
		mdc.getPipelineWriter().write(c);
		
		System.out.println("Posted your stages into Hydra");
		
	}

	private static Map<String, String> getSingleMap(String from, String to) {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put(from, to);
		
		return map;
	}
	
	public static Stage getStage(Pipeline<Stage> c, Object id, String stageName, String className, String ... afterStage) throws Exception {
		Stage s = new Stage(stageName, new DatabaseFile());
		s.getDatabaseFile().setId(id);
		
		c.addStage(s);
		Map<String, Object> props = new HashMap<String, Object>();
		props.put("stageClass", "com.findwise.hydra."+className);
		if(afterStage!=null && afterStage.length>0) {
			props.put("queryOptions", new String[]{"touched("+afterStage[0]+",true)"});
		}
		s.setProperties(props);
		return s;
	}
	
	public static Object addFile(MongoConnector dbc, String jar) throws FileNotFoundException, URISyntaxException {
		URL path = ClassLoader.getSystemResource(jar);
		File f = new File(path.toURI());
		return dbc.getPipelineWriter().save(f.getName(), new FileInputStream(f));
	}
}
