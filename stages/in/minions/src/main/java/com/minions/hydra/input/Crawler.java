package com.minions.hydra.input;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IO;

public class Crawler extends WebCrawler {
	private static String folder = "webfiles/";
	
	private static List<Pattern> visitPatterns = new ArrayList<Pattern>();
	private static List<Pattern> savePatterns = new ArrayList<Pattern>();	
	private static RemotePipeline pipeline = null;
	private static String sourceName = "";
	
	public static void setSourceName(String sourceName)
	{
		Crawler.sourceName = sourceName;
	}
	
	public static void AddVisitRegex(String regex)
	{
		visitPatterns.add(Pattern.compile(regex));
	}
	
	public static void AddSaveRegex(String regex)
	{
		savePatterns.add(Pattern.compile(regex));
	}
	
	public static synchronized void saveDocument(LocalDocument doc)
	{		
		
		if(pipeline == null)
			return;
		
		try {			
			pipeline.saveFull(doc);
		} catch (Exception e) {
			Logger.error("Crawler exception: " + e.getMessage());
		} 
	}		
	
	public static void setPipeline(RemotePipeline pipeline)
	{
		Crawler.pipeline = pipeline;
	}	
	

	@Override
    public boolean shouldVisit(WebURL url) {		
		
        String href = url.getURL();           
        
        for(Pattern p : visitPatterns)
        	if(p.matcher(href).matches())
        	{            		
        		return true;
        	}
        for(Pattern p : savePatterns)
        	if(p.matcher(href).matches())
        	{            		
        		return true;
        	}
        //Logger.debug(url.getURL() + "not visited :(");
        return false;            
    }

	@Override
    public void visit(Page page) {  			
        String url = page.getWebURL().getURL();
          
        boolean found = false;
    	for(Pattern p : savePatterns)
        	if(p.matcher(url).matches())
        	{
        		System.out.println(url);
        		found = true;
        		break;
        	}
    	   
    	if(!found) return;
    	Logger.debug("Crawler found new page to save!");
    	
    	File fol = new File(folder);
		fol.mkdir();
		
    	String hashedName = folder + CryptoHelper.MD5(url) + ".res";
    	IO.writeBytesToFile(page.getContentData(), hashedName);
    	File file = new File(hashedName);
    	
    	
    	LocalDocument dc = new LocalDocument();
    	String contenttype = page.getContentType();
		int index = contenttype.indexOf(";");
		if(index > -1)
			contenttype = contenttype.substring(0, index);
    	dc.putContentField("ContentType", contenttype);        	
    	dc.putContentField("Url", url);
    	dc.putContentField("Path", file.getAbsolutePath());
    	dc.putContentField("Date", new SimpleDateFormat().format(new Date()));
    	dc.putContentField("Source", sourceName);
		saveDocument(dc);
    }

}
