package com.minions.hydra.input;


import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

/***
 * 
 * @author joakim.uddholm
 *
 */
@Stage
public class WebCrawlStage extends AbstractInputStage   {
	
	// TODO Add parameters for these crawler settings
	private String UserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0";
	private String Root = "crawlertmp/"; 
	private int NumberOfCrawlers = 5;
	private int PolitenessDelay = 1000;
	private int MaxCrawlingDepth = -1;	
	private boolean IncludeBinaryContentInCrawling = true;	

	@Parameter(name = "saveRegexes", description = "List of regexes of which urls to save to file")
	private List<String> saveRegexes = new ArrayList<String>();
	
	@Parameter(name = "visitRegexes", description = "List of regexes of which urls to visit")
	private List<String> visitRegexes = new ArrayList<String>();
	
	@Parameter(name = "urls", description = "List of urls to begin crawling from")
	private List<String> urls = new ArrayList<String>();
	
	@Parameter(name="wait", description="Delay between crawls")
	private int wait = 30*60*1000;	
	
	@Parameter(name="sourceName", description="")
	private String sourceName;
	
	@Override
	public void init() throws RequiredArgumentMissingException
	{
		Logger.info("Starting WebCrawlStage");		
		try {
			
			while(true)
			{
				CrawlConfig Config  = new CrawlConfig();
				Config.setPolitenessDelay(PolitenessDelay);
				Config.setCrawlStorageFolder(Root+sourceName);
				Config.setUserAgentString(UserAgent);
				Config.setIncludeBinaryContentInCrawling(IncludeBinaryContentInCrawling);        
				Config.setMaxDepthOfCrawling(MaxCrawlingDepth);
		        
				PageFetcher fetcher= new PageFetcher(Config);		
				RobotstxtConfig robotcfg = new RobotstxtConfig();
		        RobotstxtServer robotsrv = new RobotstxtServer(robotcfg, fetcher);		
				
				CrawlController controller = new CrawlController(Config, fetcher, robotsrv);				
				for(String seed: urls)
					controller.addSeed(seed);	
				
				for(String regex: saveRegexes)
					Crawler.AddSaveRegex(regex);			
	
				for(String regex: visitRegexes)
					Crawler.AddVisitRegex(regex);
				
				Crawler.setPipeline(getRemotePipeline());	
				Crawler.setSourceName(sourceName);
	
				Logger.info("Starting new crawl-session with " + NumberOfCrawlers);
				controller.start(Crawler.class, NumberOfCrawlers);
				Thread.sleep(wait);
			}
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return;
		}	
		
	}

}
