package com.findwise.hydra;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.HashMap;

import com.findwise.hydra.Stage.Mode;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.local.LocalQuery;

public class SetupSleepyPipeline {
	public static void main(String[] args) throws Exception {
		int numberOfStages = 10;
		int timeToSleep = 10;
		
		URL url = ClassLoader.getSystemResource("sleepy-stage-jar-with-dependencies.jar");
		File f;
		if(url == null) {
			f = new File("/home/findwise/shipit/sleepy-stage-jar-with-dependencies.jar");
		} else {
			f = new File(url.toURI());
		}
		
		DatabaseConnector<?> dbc = Utility.getConnectorInstance();
		
		dbc.getPipelineWriter().save("sleepy", "sleepy.jar", new FileInputStream(f));
		
		Pipeline p = dbc.getPipelineReader().getPipeline();
		for(StageGroup g : p.getStageGroups()) {
			p.removeGroup(g.getName());
		}
		if(p.getStages().size() > 0) {
			throw new IllegalStateException();
		}
		

		LocalQuery query = new LocalQuery();
		for(int i=0; i<numberOfStages; i++) {
			StageGroup g = new StageGroup("sleepyGroup"+i);
			Stage s = new Stage("sleepy"+i, dbc.getPipelineReader().getFile("sleepy.jar"));
			s.setMode(Mode.ACTIVE);
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put("stageClass", "com.findwise.hydra.stage.SleepyStage");
			map.put("timeToSleep", timeToSleep);
			s.setProperties(map);
			g.addStage(s);
			query.requireTouchedByStage("sleepy"+i);
			p.addGroup(g);
		}
		
		
		StageGroup g = new StageGroup("output");
		Stage syso = new Stage("system-out", dbc.getPipelineReader().getFile("sleepy.jar"));

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("stageClass", "com.findwise.hydra.stage.SystemOutputStage");
		map.put("query", SerializationUtils.toObject(query.toJson()));
		syso.setProperties(map);
		g.addStage(syso);
		p.addGroup(g);
		
		dbc.getPipelineWriter().write(p);
	}
}
