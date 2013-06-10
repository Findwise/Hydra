package com.minions.hydra.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

@Stage
public class RSSInputStage extends AbstractInputStage {

	@Parameter(name = "url", description = "Url to get rss feed from")
	private String url = "";
	
	@Parameter(name = "wait", description = "Time to wait between requests")
	private int wait = 60*1000*30;	
	
	@Parameter(name="sourceName", description="")
	private String sourceName;
	
	@Override
	public void init() throws RequiredArgumentMissingException {
		Logger.debug("Starting rss stage");
		while(true)
		{
			InputStream in = null;
			HttpURLConnection con = null;
			try {
				con = (HttpURLConnection) (new URL(url)).openConnection();						
				in = con.getInputStream();		
			}
			catch(IOException ex)
			{
				Logger.error("Connection to "+url+" failed");
				return;
			} 
			
			try {
				Logger.debug("Got stream from url " + url);
				List<LocalDocument> docs = parseRSS(in);
				
				for(LocalDocument doc : docs) {
					doc.putContentField("Source", sourceName);
					getRemotePipeline().saveFull(doc);
				}
				
				Logger.info("Got " + docs.size() + " items from RSS feed.");
			} catch (XMLStreamException e) {
				Logger.error(e.toString());				
				return;
			} catch (IOException e) {				
				Logger.error(e.toString());
				return;
			} catch (JsonException e) {				
				Logger.error(e.toString());
				return;
			}
			
			try {
				Thread.sleep(wait);
			} catch(InterruptedException ex)
			{
				Logger.error("Interrupted exception. Avslutar.");
				return;
			}
			
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	// TODO: Troligen fel att läsa strömmen från socket direkt hit. Fixa sen!
	public List<LocalDocument> parseRSS(InputStream in) throws XMLStreamException, IOException, JsonException
	{
		XMLInputFactory fact = XMLInputFactory.newFactory();
		XMLStreamReader reader = fact.createXMLStreamReader(in);
		
		Logger.debug("Got reader");
		
		ArrayList<LocalDocument> docs = new ArrayList<LocalDocument>();
		LocalDocument doc = null;
		while(reader.hasNext())
		{		
			reader.next();
			
			/*
			if(reader.hasText())
				Logger.debug(reader.getText());
			*/
			
			if(reader.isStartElement())
			{
				String tag = reader.getLocalName();
				//Logger.debug(tag);
				if(reader.getLocalName().equals("item")) {
					doc = new LocalDocument();
					continue;
				}

				if(doc == null)
					continue;
				
				String val = reader.getElementText();
				doc.putContentField(tag, val);
		
			} 
			else if(reader.isEndElement())
			{
				if(reader.getLocalName().equals("item")) {
					//Logger.info("Saved new document");
					docs.add(doc);
					doc = null;
				}				
			}
		}
		
		return docs;
		
	}
	
	
}
