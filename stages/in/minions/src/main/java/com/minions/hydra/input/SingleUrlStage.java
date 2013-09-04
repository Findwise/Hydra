package com.minions.hydra.input;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

/**
 * inputConfiguration={rssUrl:\"\",wait:1800000}
 * 
 * rssUrl: Url to get rss feed from
 * 
 * wait: Time to wait between requests
 * 
 * @author joakim.uddholm
 * 
 */
@Stage
public class SingleUrlStage extends AbstractInputStage {
	
	private static String folder = "webfiles/";
	
	@Parameter(name = "url", description = "Url to get rss feed from")
	private String url = "";
	
	@Parameter(name = "wait", description = "Time to wait between requests")
	private int wait = 60*1000*30;	
	
	@Parameter(name="sourceName", description="")
	private String sourceName;
	
	public void init() throws RequiredArgumentMissingException {
		Logger.info("Starting Single Url Input Stage");		

		while(true)
		{					
			try {
				HttpURLConnection con = (HttpURLConnection) (new URL(url)).openConnection();			
				InputStream in = con.getInputStream();						
				StringBuilder builder = new StringBuilder();				
				
				String hashedName = folder + CryptoHelper.MD5(url) + ".res";
				File fol = new File(folder);
				fol.mkdir();
				File file = new File(hashedName);
	            FileOutputStream fs = new FileOutputStream(file);
	            
            	byte[] buffer = new byte[4096];
				int len = 0;
				while ((len = in.read(buffer)) > 0) 
				{
					fs.write(buffer, 0, len);													
				}		
				fs.close();
				
				LocalDocument dc = new LocalDocument();				
				dc.putContentField("Url", url);
				dc.putContentField("Date", new SimpleDateFormat().format(new Date()));
				String contenttype = con.getContentType();
				int index = contenttype.indexOf(";");
				if(index > -1)
					contenttype = contenttype.substring(0, index);
				dc.putContentField("Content-Type", contenttype);				
				dc.putContentField("Path", file.getAbsolutePath());
				dc.putContentField("Source", sourceName);
				getRemotePipeline().saveFull(dc);
				
				Thread.sleep(wait);
			} catch (MalformedURLException e1) {
				Logger.error("Bad URL: " + url);						
				break;
			} catch (IOException e1) {
				Logger.error("IOException occured while fetching from url: " + e1.toString());					
				break;
			} catch (InterruptedException e) {						
				// Will this ever happen? If so continue?
			} catch (JsonException e) {
				Logger.error("Error while generating JSON.");
				break;
			}					
			
		}		
		
	}

}
