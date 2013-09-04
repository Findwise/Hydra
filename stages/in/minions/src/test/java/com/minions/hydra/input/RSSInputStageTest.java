package com.minions.hydra.input;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;

public class RSSInputStageTest {

	InputStream in;
	
	@Before
	public void setUp() throws Exception {
		in = new FileInputStream("polisen.xml");
	}

	@Test
	public void testParseRSS() {
		RSSInputStage stage = new RSSInputStage();
		List<LocalDocument> docs = null;
		try {
			docs = stage.parseRSS(in);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			return; 
		}
		
		assert(docs.size() == 200);
			
	}

}
