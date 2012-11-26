package com.findwise.hydra.stage.webstages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.findwise.hydra.stage.Parameter;
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
public class JsoupAttrSelector extends AbstractJsoupSelector {

	/**
	 * Takes a jsoup Document and gets all the elements that matches the 
	 * selector and returns a list of strings with the elements' attribute 
	 * text values
	 * 
	 * @param jsoupDoc
	 * @param jSoupConfig
	 * @return A list of strings containing the text values of the elements' 
	 *         attributes selected
	 */
	@Override
	ArrayList<String> getJsoupElements(Document jsoupDoc,
			Map<String, String> jSoupConfig) {
		String selector = jSoupConfig.get("selector");
		String attribute = jSoupConfig.get("attribute");
		
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
	 * selector and returns its selected attribute
	 * 
	 * @param jsoupDoc
	 * @param jSoupConfig
	 * @return The value of the selected attribute for the selected element
	 */
	String getJsoupElement(Document jsoupDoc, Map<String, String> jSoupConfig) {
		String selector = jSoupConfig.get("selector");
		String attribute = jSoupConfig.get("attribute");
		
		if(jsoupDoc.select(selector).size() > 0) {
			if (jsoupDoc.select(selector).first().hasAttr(attribute)){
				return jsoupDoc.select(selector).first().attr(attribute);
			}
		}
		return "";
	}
	
}
