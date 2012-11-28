package com.findwise.hydra.stage;

import java.util.Map;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

@Stage
public class SystemOutputStage extends AbstractOutputStage {


	@Override
	public void output(LocalDocument document) {
		System.out.println(document);
		
		try {
			getRemotePipeline().markProcessed(document);
		} catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * For convenience...
	 * @param args
	 */
	public static void main(String[] args) {
		//AbstractStage.main(args);
		try {
			SystemOutputStage ios = new SystemOutputStage();
			RemotePipeline rp = new RemotePipeline("system-out");
			Map<String, Object> map = rp.getProperties();
			System.out.println(map);
			map.put("stageClass", ios.getClass().getName());
			ios.setUp(new RemotePipeline("system-out"), map);
			ios.start();
			System.out.println("query is: "+ios.getQuery());
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
