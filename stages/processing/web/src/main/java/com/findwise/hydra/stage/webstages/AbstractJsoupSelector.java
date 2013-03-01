package com.findwise.hydra.stage.webstages;

import com.findwise.hydra.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;

/**
 * This stage uses JSoup and it's DOM selector queries (http://jsoup.org
 * contains selector documentation) to parse HTML code from a specified field
 * stored in the LocalDocument in hydra. 
 * 
 * It provides all the processing and selector operations for JSoup so the
 * extending classes can retrieve anything from the selected content.
 * 
 * selector = the jsoup selector query 
 * fieldname = the fieldname where you want the result from the query stored in (Hydras mongodb storage) 
 * singlevalue = if you want to only get the first element found or all of them
 * 
 * The example given would get the first h1 tag in the HTML and put it in the
 * Hydra store with the fieldname "h1". Then it would get all the h2 tags and
 * store them in Hydra store with fieldname "h2"
 * 
 * @author jens.bengtsson
 * 
 */
public abstract class AbstractJsoupSelector extends AbstractProcessStage {

	@Parameter(name = "htmlField", required = true, description = "The input field containing HTML/XML, or a listfield with fields containing HTML/XML")
	private String htmlField;

	@Parameter(name = "jSoupConfigs", required = true, description = "List of configs, where each config is a map with at least the keys 'selector', 'fieldname' and optionally 'singlevalue' (only output the first selected element; false if omitted)")
	private List<Map<String, String>> jSoupConfigs;

	@Override
	public void init() throws RequiredArgumentMissingException {
		
		if (this.htmlField == null || htmlField.length() == 0)
			throw new RequiredArgumentMissingException("htmlField missing");

		if (jSoupConfigs == null || jSoupConfigs.size() == 0)
			throw new RequiredArgumentMissingException("jSoupConfigs missing");
		
	}

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		
        Object value = doc.getContentField(htmlField);
        
        if(value instanceof String){
            String content = (String) doc.getContentField(htmlField);
            jsoupParse(doc, content, false);
        } else if (value instanceof List<?>){
        	boolean append = false;
            for (Object val : (List<?>)value) {
				if (val instanceof String) {
                    jsoupParse(doc, (String)val, append);
                } else {
                    Logger.warn("Field " + htmlField + " was a list but not a List<String>");
                }
				if (!append) append = true;
            }
        } else {
            Logger.warn("Field " + htmlField + " did not contain String or List<String>");         
        }
  	}

    private void jsoupParse(LocalDocument doc, String content, boolean append) {
    	if(content != null) {
			Document jsoupDoc = Jsoup.parse(content);
			
			for (Map<String, String> jsoupConfig : jSoupConfigs) {
				String fieldName = jsoupConfig.get("fieldname");
				if (jsoupConfig.get("singlevalue") != null && jsoupConfig.get("singlevalue").equalsIgnoreCase("true")){
					String fieldContent = getJsoupElement(jsoupDoc, jsoupConfig);
					if (fieldContent != null && !fieldContent.isEmpty()){
						doc.putContentField(fieldName, fieldContent);
					}
				} else {
					List<String> fieldContent = new ArrayList<String>();
					List<String> selectedContent = getJsoupElements(jsoupDoc, jsoupConfig);
					if (append) {
						@SuppressWarnings("unchecked")
						List<String> oldFieldContent = doc.hasContentField(fieldName) ? (List<String>)doc.getContentField(fieldName) : new ArrayList<String>();
						fieldContent.addAll(oldFieldContent);
					}
					fieldContent.addAll(selectedContent);
					if (!fieldContent.isEmpty()){
						doc.putContentField(fieldName, fieldContent);
					}
				}
			}
		}
    
    }
    
    
	/**
	 * Takes a jsoup Document and gets all the elements that matches the
	 * selector and returns a list of the selected content
	 * 
	 * @param jsoupDoc
	 * @param selector
	 * @return A list of strings containing the text values of the elements
	 *         selected
	 */
	abstract ArrayList<String> getJsoupElements(Document jsoupDoc,
			Map<String, String> jSoupConfig);

	/**
	 * Takes a jsoup Document and gets the first instance that matches the
	 * selector and returns whatever selected
	 * 
	 * @param jsoupDoc
	 * @param selector
	 * @return The text (without HTML code), or the HTML representation if getHTML is true, from the selected element
	 */
	abstract String getJsoupElement(Document jsoupDoc, Map<String, String> jSoupConfig);

	public List<Map<String, String>> getjSoupConfigs() {
		return jSoupConfigs;
	}

	public void setjSoupConfigs(List<Map<String, String>> jSoupConfigs) {
		this.jSoupConfigs = jSoupConfigs;
	}

	public String getHtmlField() {
		return htmlField;
	}

	public void setHtmlField(String htmlField) {
		this.htmlField = htmlField;
	}
}
