package com.findwise.hydra.stage;

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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DocumentDiscardStageTest {
	
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
    	discardStage  = new DocumentDiscardStage();
    }
    
    @Before 
    public void setUp() throws Exception { 	
    	mockedDoc1 = spy(new LocalDocument());
    	mockedDoc2 = spy(new LocalDocument());
    	mockedDoc3 = spy(new LocalDocument());
    	mockedDoc4 = spy(new LocalDocument());
    	mockedDoc5 = spy(new LocalDocument());
    	mockedDoc6 = spy(new LocalDocument());
    	mockedDoc7 = spy(new LocalDocument());
    	mockedDoc8 = spy(new LocalDocument());
    	mockedDoc9 = spy(new LocalDocument());
    	mockedDoc10 = spy(new LocalDocument());

		doReturn("http://www.abc.se/def123/index.html").when(mockedDoc1).getContentField("url");
    	doReturn("xyz.prs.com/se").when(mockedDoc2).getContentField("url");
    	doReturn("www.foo.se").when(mockedDoc3).getContentField("url");
    	doReturn("www.foo.co.uk/se/bar.html").when(mockedDoc4).getContentField("url");
    	doReturn("se.findwise.hydra").when(mockedDoc5).getContentField("url");
    	doReturn("www.foo.se/").when(mockedDoc6).getContentField("url");
    	doReturn("www.foo.org/").when(mockedDoc7).getContentField("url");
    	doReturn("www.foo.com/").when(mockedDoc8).getContentField("url");
    	doReturn("seb").when(mockedDoc9).getContentField("FIELD_DISPLAY_NAME");
    	ArrayList<String> list = new ArrayList<String>();
    	list.add("Seb");
    	list.add("Test");
    	doReturn(list).when(mockedDoc10).getContentField("FIELD_DISPLAY_NAME");

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
		verify(mockedDoc1).discard();

		discardStage.process(mockedDoc2);
		verify(mockedDoc2, times(0)).discard();

		discardStage.process(mockedDoc3);
		verify(mockedDoc3).discard();
		
		discardStage.process(mockedDoc4);
		verify(mockedDoc4, times(0)).discard();

		discardStage.process(mockedDoc5);
		verify(mockedDoc5, times(0)).discard();

		discardStage.process(mockedDoc6);
		verify(mockedDoc6).discard();

		discardStage.process(mockedDoc7);
		verify(mockedDoc7, times(0)).discard();
		
		discardStage.process(mockedDoc8);
		verify(mockedDoc8, times(0)).discard();
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
		verify(mockedDoc1).discard();
		verify(mockedDoc1, times(2)).getContentField("url");
				
		discardStage.process(mockedDoc2);
		verify(mockedDoc2).discard();
		verify(mockedDoc2, times(1)).getContentField("url");
		
		discardStage.process(mockedDoc3);
		verify(mockedDoc3).discard();
		verify(mockedDoc3, times(2)).getContentField("url");

		discardStage.process(mockedDoc4);
		verify(mockedDoc4, times(0)).discard();
		verify(mockedDoc4, times(3)).getContentField("url");

		discardStage.process(mockedDoc5);
		verify(mockedDoc5, times(0)).discard();
		verify(mockedDoc5, times(3)).getContentField("url");

		discardStage.process(mockedDoc6);
		verify(mockedDoc6).discard();
		verify(mockedDoc6, times(2)).getContentField("url");
		
		discardStage.process(mockedDoc7);
		verify(mockedDoc7).discard();
		verify(mockedDoc7, times(3)).getContentField("url");
		
		discardStage.process(mockedDoc8);
		verify(mockedDoc8).discard();
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
		verify(mockedDoc9,Mockito.never()).discard();
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
		verify(mockedDoc10,Mockito.atLeastOnce()).discard();
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
		verify(mockedDoc10,Mockito.atLeastOnce()).discard();
	}
	
}
