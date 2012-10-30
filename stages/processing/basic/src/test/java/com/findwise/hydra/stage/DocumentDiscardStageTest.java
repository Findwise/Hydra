package com.findwise.hydra.stage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class DocumentDiscardStageTest {
	
	private static RemotePipeline mockedPipeline; 
	private static DocumentDiscardStage discardStage;
	
	private LocalDocument mockedDoc1;
	private LocalDocument mockedDoc2; 
	private LocalDocument mockedDoc3;
	private LocalDocument mockedDoc4;
	private LocalDocument mockedDoc5;
	private LocalDocument mockedDoc6;
	private LocalDocument mockedDoc7;
	private LocalDocument mockedDoc8;
	private LocalDocument mockedDoc9;
	private LocalDocument mockedDoc10;
	
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    	mockedPipeline = mock(RemotePipeline.class);
    	discardStage  = new DocumentDiscardStage();
    	discardStage.setRemotePipeline(mockedPipeline);
    	// assumption that remote pipeline works properly
    	when((mockedPipeline).markDiscarded(any(LocalDocument.class))).thenReturn(true);
    }
    
    @Before 
    public void setUp() throws Exception { 	
    	mockedDoc1 = mock(LocalDocument.class);
    	mockedDoc2 = mock(LocalDocument.class);
    	mockedDoc3 = mock(LocalDocument.class);
    	mockedDoc4 = mock(LocalDocument.class);
    	mockedDoc5 = mock(LocalDocument.class);
    	mockedDoc6 = mock(LocalDocument.class);
    	mockedDoc7 = mock(LocalDocument.class);
    	mockedDoc8 = mock(LocalDocument.class);
    	mockedDoc9 = mock(LocalDocument.class);
    	mockedDoc10 = mock(LocalDocument.class);
    	
    	when(mockedDoc1.getContentField("url")).thenReturn("http://www.abc.se/def123/index.html"); 
    	when(mockedDoc2.getContentField("url")).thenReturn("xyz.prs.com/se"); 
    	when(mockedDoc3.getContentField("url")).thenReturn("www.foo.se"); 
    	when(mockedDoc4.getContentField("url")).thenReturn("www.foo.co.uk/se/bar.html"); 
    	when(mockedDoc5.getContentField("url")).thenReturn("se.findwise.hydra"); 
    	when(mockedDoc6.getContentField("url")).thenReturn("www.foo.se/"); 
    	when(mockedDoc7.getContentField("url")).thenReturn("www.foo.org/"); 
    	when(mockedDoc8.getContentField("url")).thenReturn("www.foo.com/");
    	when(mockedDoc9.getContentField("FIELD_DISPLAY_NAME")).thenReturn("seb");
    	ArrayList<String> list = new ArrayList<String>();
    	list.add("Seb");
    	list.add("Test");
    	when(mockedDoc10.getContentField("FIELD_DISPLAY_NAME")).thenReturn(list);
    	
    }

	@Test
	public void testSingleConfig() throws RequiredArgumentMissingException, ProcessException, HttpException, IOException, IllegalArgumentException, IllegalAccessException {
		// set test properties and initialize stage
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "url");
		config.put("regex", ".*[.]se.*");
		
		configs.add(config);
		
		discardStage.setDiscardConfigs(configs);

		// test process method
		discardStage.process(mockedDoc1);
		verify(mockedPipeline).markDiscarded(mockedDoc1);
		
		discardStage.process(mockedDoc2);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc2);
		
		discardStage.process(mockedDoc3);
		verify(mockedPipeline).markDiscarded(mockedDoc3);
		
		discardStage.process(mockedDoc4);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc4);

		discardStage.process(mockedDoc5);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc5);

		discardStage.process(mockedDoc6);
		verify(mockedPipeline).markDiscarded(mockedDoc6);

		discardStage.process(mockedDoc7);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc7);
		
		discardStage.process(mockedDoc8);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc8);
	}
	
	@Test
	public void testMultipleConfigs() throws RequiredArgumentMissingException, ProcessException, HttpException, IOException, IllegalArgumentException, IllegalAccessException {
		// set test properties and initialize stage
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("field", "url");
		config1.put("regex", ".*\\.com.*");
		Map<String, String> config2 = new HashMap<String, String>();
		config2.put("field", "url");
		config2.put("regex", ".*\\.se.*");
		Map<String, String> config3 = new HashMap<String, String>();
		config3.put("field", "url");
		config3.put("regex", ".*\\.org.*");
		
		configs.add(config1);
		configs.add(config2);
		configs.add(config3);	
		
		discardStage.setDiscardConfigs(configs);
	   	
		// test process method
		discardStage.process(mockedDoc1);
		verify(mockedPipeline).markDiscarded(mockedDoc1);
		verify(mockedDoc1, times(2)).getContentField("url");
				
		discardStage.process(mockedDoc2);
		verify(mockedPipeline).markDiscarded(mockedDoc2);
		verify(mockedDoc2, times(1)).getContentField("url");
		
		discardStage.process(mockedDoc3);
		verify(mockedPipeline).markDiscarded(mockedDoc3);
		verify(mockedDoc3, times(2)).getContentField("url");

		discardStage.process(mockedDoc4);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc4);
		verify(mockedDoc4, times(3)).getContentField("url");

		discardStage.process(mockedDoc5);
		verify(mockedPipeline, times(0)).markDiscarded(mockedDoc5);
		verify(mockedDoc5, times(3)).getContentField("url");

		discardStage.process(mockedDoc6);
		verify(mockedPipeline).markDiscarded(mockedDoc6);
		verify(mockedDoc6, times(2)).getContentField("url");
		
		discardStage.process(mockedDoc7);
		verify(mockedPipeline).markDiscarded(mockedDoc7);
		verify(mockedDoc7, times(3)).getContentField("url");
		
		discardStage.process(mockedDoc8);
		verify(mockedPipeline).markDiscarded(mockedDoc8);
		verify(mockedDoc8, times(1)).getContentField("url");
	}
	
	@Test
	public void testNoHitsNameDiscard() throws ProcessException, IOException, HttpException{
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "FIELD_DISPLAY_NAME");
		config.put("regex", "ADMIN seb");
		configs.add(config);
		
		discardStage.setDiscardConfigs(configs);
		
		discardStage.process(mockedDoc9);
		verify(mockedPipeline,Mockito.never()).markDiscarded(mockedDoc9);		
	}
	
	@Test
	public void testMultipleValuedField() throws ProcessException, IOException, HttpException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "FIELD_DISPLAY_NAME");
		config.put("regex", "Seb");
		configs.add(config);
		
		discardStage.setDiscardConfigs(configs);
		
		discardStage.process(mockedDoc10);
		verify(mockedPipeline,Mockito.atLeastOnce()).markDiscarded(mockedDoc10);
	}


	@Test
	public void testMultipleValuedFieldSecond() throws ProcessException, IOException, HttpException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "FIELD_DISPLAY_NAME");
		config.put("regex", "Test");
		configs.add(config);
		
		discardStage.setDiscardConfigs(configs);
		
		discardStage.process(mockedDoc10);
		verify(mockedPipeline,Mockito.atLeastOnce()).markDiscarded(mockedDoc10);
	}
	
}
