package com.findwise.hydra;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Stage;

@Stage
public class InteractiveOutputStage extends AbstractOutputStage {
	
	@Override
	public void output(LocalDocument document) {
		try {
			System.out.print("(p)rocessed, (d)elete, (f)ailed. Select action: ");
			String s = new Scanner(System.in).nextLine().toLowerCase(Locale.ENGLISH);
			if(s.equals("d")) {
				System.out.println("Discarding the document");
				getRemotePipeline().markDiscarded(document);
			} else if(s.equals("p")) {
				System.out.println("Processing the document");
				getRemotePipeline().markProcessed(document);
			} else if(s.equals("f")) {
				System.out.println("Failing the document");
				getRemotePipeline().markFailed(document);
			} else {
				System.out.println("Unknown command: "+s);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * For convenience...
	 * @param args
	 */
	public static void main(String[] args) {
		//AbstractStage.main(args);
		try {
			InteractiveOutputStage ios = new InteractiveOutputStage();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("stageClass", ios.getClass().getName());
			ios.setUp(new RemotePipeline("interactive"), map);
			ios.start();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
