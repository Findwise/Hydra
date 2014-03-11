/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.findwise.hydra.stage.webstages;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 *
 * @author Roar Granevang
 */
public class RenderHTMLStageTest {

	public RenderHTMLStageTest() {
	}

	@Test
	public void testProcessEmptyField() throws Exception {
		InputStream is = RenderHTMLStageTest.class.getResourceAsStream("/htmlDocument.html");
		StringWriter writer = new StringWriter();
		IOUtils.copy(is, writer, "utf-8");
		String htmlString = writer.toString();

		RenderHTMLStage instance = new RenderHTMLStage();

        List<String> inputFields = new LinkedList<String>();
        inputFields.add("html");
        inputFields.add("body");
        instance.setFields(inputFields);

		LocalDocument ld = new LocalDocument();
		
		instance.init();
		instance.process(ld);

		//Only really want to test that it doesn't throw a nullpointer when inputfields are empty
		Assert.assertFalse(htmlString.equals(ld.getContentField("html")));
		Assert.assertFalse(htmlString.equals(ld.getContentField("body")));
	}

	@Test
	public void testProcessSettingOnlyOneInputField() throws Exception {
		InputStream is = RenderHTMLStageTest.class.getResourceAsStream("/htmlDocument.html");
		StringWriter writer = new StringWriter();
		IOUtils.copy(is, writer, "utf-8");
		String htmlString = writer.toString();

		RenderHTMLStage instance = new RenderHTMLStage();

        List<String> inputFields = new LinkedList<String>();
        inputFields.add("html");
        inputFields.add("body");
        instance.setFields(inputFields);

		LocalDocument ld = new LocalDocument();
		
		
		ld.putContentField("html", htmlString);
		ld.putContentField("body", htmlString);

		Assert.assertEquals(htmlString, ld.getContentField("html"));
		Assert.assertEquals(htmlString, ld.getContentField("body"));

		instance.init();
		instance.process(ld);

		Assert.assertFalse(htmlString.equals(ld.getContentField("html")));
		Assert.assertFalse(htmlString.equals(ld.getContentField("body")));
	}

	@Test(expected = RequiredArgumentMissingException.class)  
	public void testInputFieldNotSet() throws RequiredArgumentMissingException{
		RenderHTMLStage instance = new RenderHTMLStage();
		instance.init();
	}
}