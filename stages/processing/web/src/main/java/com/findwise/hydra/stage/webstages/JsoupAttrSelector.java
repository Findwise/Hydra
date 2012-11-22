package com.findwise.hydra.stage.webstages;

import com.findwise.hydra.common.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

/**
 * This stage uses JSoup and it's DOM selector queries (http://jsoup.org
 * contains selector documentation) to parse HTML code from a specified field
 * stored in the LocalDocument in hydra and retrieve a certain attribute from them. 
 * This can be used for e.g. parsing out all a tags from a page and retrieve all 
 * the href attributes (meaning all the output hyperlinks of the page)
 * 
 * The stage takes its configuration from a properties-file and needs the
 * following arguments: 
 * htmlField = the field stored in Hydras mongodb storage
 * that contains the HTML-code you want to parse (can be a list or a string field)
 * jSoupConf = A Json string
 * containing multiple selector configurations. It uses the following format:
 * [{selector:h1,attribute:class,fieldname:h1,singlevalue:true},
 * {selector:h2,attribute:class,fieldname:h2,singlevalue:false}]
 * 
 * selector = the jsoup selector query
 * attribute = the attribute to retrieve from the elements matching selector query 
 * fieldname = the fieldname where you want the result from the query stored in (Hydras mongodb storage) 
 * singlevalue = if you want to only get the first element found or all of them
 * 
 * The example given would get the class attribute of the first h1 tag in the 
 * HTML and put it in the Hydra store with the fieldname "h1". Then it would 
 * get all the h2 tags and store their class attributes them in Hydra store 
 * with fieldname "h2".
 * 
 * @author jens.bengtsson
 * 
 */
@Stage(description = "This stage uses JSoup and DOM selectors to parse HTML/XML from a field and output the selected elements (text content or raw HTML/XML) as a list (or optionally, the first element that matches).")
public class JsoupAttrSelector extends AbstractProcessStage {

	@Parameter(name = "htmlField", description = "The input field containing HTML/XML, or a listfield with fields containing HTML/XML")
	private String htmlField;
	
	@Parameter(name = "jSoupConfigs", description = "List of configs, where each config is a map with the keys 'selector', 'fieldname', 'attribute' and optionally 'singlevalue' (only output the first selected element; false if omitted)")
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
				if (jsoupConfig.get("singlevalue") != null && jsoupConfig.get("singlevalue").equalsIgnoreCase("true")) {
					doc.putContentField(jsoupConfig.get("fieldname"),
							getJsoupElementAttribute(jsoupDoc, jsoupConfig.get("selector"), jsoupConfig.get("attribute")));
				} else {
					String fieldName = jsoupConfig.get("fieldname");
					List<String> fieldContent = new ArrayList<String>();
					List<String> selectedContent = getJsoupElements(jsoupDoc, jsoupConfig.get("selector"), jsoupConfig.get("attribute"));
					if (append) {
						@SuppressWarnings("unchecked")
						List<String> oldFieldContent = (List<String>)doc.getContentField(fieldName);
						fieldContent.addAll(oldFieldContent);
						fieldContent.addAll(selectedContent);
					} else {
						fieldContent.addAll(selectedContent);
					}
					doc.putContentField(fieldName, fieldContent);
				}
			}
		}
    
    }
    
    
	/**
	 * Takes a jsoup Document and gets all the elements that matches the
	 * selector and returns a list of strings with the elements text values
	 * 
	 * @param jsoupDoc
	 * @param selector
	 * @return A list of strings containing the text values of the elements
	 *         selected
	 */
	private ArrayList<String> getJsoupElements(Document jsoupDoc,
			String selector, String attribute) {
		ArrayList<String> fieldList = new ArrayList<String>();

		Elements elems = jsoupDoc.select(selector);

		for (Element element : elems) {
			if (element.hasAttr(attribute)) {
				fieldList.add(element.attr(attribute));
			}
		}

		return fieldList;
	}

	/**
	 * Takes a jsoup Document and gets the first instance that matches the
	 * selector and returns its text or, if getHTML is true, its HTML representation
	 * 
	 * @param jsoupDoc
	 * @param selector
	 * @return The text (without HTML code), or the HTML representation if getHTML is true, from the selected element
	 */
	private String getJsoupElementAttribute(Document jsoupDoc, String selector, String attribute) {
		if(jsoupDoc.select(selector).size() > 0) {
			if (jsoupDoc.select(selector).first().hasAttr(attribute)){
				return jsoupDoc.select(selector).first().attr(attribute);
			}
		}
		return "";
	}

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
