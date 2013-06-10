package com.minions.hydra.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.Stage;

@Stage
public class JSONFileStage extends AbstractInputStage {

	@Parameter(name = "MonitorPath", description = "Path of the folder to monitor")
	public String MonitorPath;
	
	@Parameter(name = "MonitorDelayValue", description = "Delay time for monitor between monitors")
	public int MonitorDelayValue;    
	
	@Override
	public void init() {		
		Logger.info("Starting JSON File monitor");
		File inputFolder = new File(MonitorPath);
		
		if(!inputFolder.exists())		
			inputFolder.mkdir();		
		
		for(;;)
		{
			File[] files = inputFolder.listFiles();
			for(File f : files)
			{
				if(f.getName().indexOf(".json") == -1)
					continue;				
				
				Logger.info("Parsing " + f.getName());
				StringBuilder builder = new StringBuilder();
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
					
					String input = null;
					while((input = reader.readLine()) != null)
						builder.append(input);
				} catch (FileNotFoundException e) {
					// Very improbable, unless the file we found just disappeared...
					e.printStackTrace();
					continue;
				} catch (IOException e) {					
					e.printStackTrace();
					return;
				} finally {
					try {
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				LocalDocument doc = new LocalDocument();				
				try {
					doc.fromJson(builder.toString());
					getRemotePipeline().saveFull(doc);
				} catch (JsonException e) {
					Logger.error("Bad JSON in "+f.getName()+": " + e.getMessage());
					continue;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}				
				f.delete();
			}
			
			try {
				Thread.sleep(MonitorDelayValue);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
