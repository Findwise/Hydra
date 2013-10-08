package com.findwise.hydra.stage.webstages;

import java.util.ArrayList;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.Stage;

/**
 * This stage uses JSoup and it's DOM selector queries (http://jsoup.org
 * contains selector documentation) to parse HTML code from a specified field
 * stored in the LocalDocument in hydra. This can be used for e.g. parsing out
 * all h1 tags from a page or a specific div with the body of copy that you want
 * to index.
 * 
 * The stage takes its configuration from a properties-file and needs the
 * following arguments: 
 * htmlField = the field stored in Hydras mongodb storage
 * that contains the HTML-code you want to parse (can be a list or a string field)
 * jSoupConf = A Json string
 * containing multiple selector configurations. It uses the following format:
 * [{selector
 * :h1,fieldname:h1,singlevalue:true},{selector:h2,fieldname:h2,singlevalue
 * :false}]
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
@Stage(description = "This stage uses JSoup and DOM selectors to parse HTML/XML from a field and output the selected elements (text content or raw HTML/XML) as a list (or optionally, the first element that matches).")
public class JsoupSelector extends AbstractJsoupSelector {

	@Parameter(name = "returnHTML", description = "Switch for outputting raw HTML or not, defaults to FALSE")
	private boolean returnHTML = false;

	private static final String HTML_OUTPUT_CLEANUP = "\\n\\s*";

	/**
	 * Takes a jsoup Document and gets all the elements that matches the
	 * selector and returns a list of strings with the elements text values
	 * 
	 * @param jsoupDoc
	 * @param jSoupConfig
	 * @return A list of strings containing the text values of the elements
	 *         selected
	 */
	@Override
	ArrayList<String> getJsoupElements(Document jsoupDoc,
			Map<String, String> jSoupConfig) {
		String selector = jSoupConfig.get("selector");
		ArrayList<String> fieldList = new ArrayList<String>();

		Elements elems = jsoupDoc.select(selector);
		for (Element element : elems) {
			if (returnHTML) {
				fieldList.add(element.outerHtml().replaceAll(HTML_OUTPUT_CLEANUP, ""));
			} else {
				fieldList.add(element.text());
			}
		}

		return fieldList;
	}

	/**
	 * Takes a jsoup Document and gets the first instance that matches the
	 * selector and returns its text or, if getHTML is true, its HTML representation
	 * 
	 * @param jsoupDoc
	 * @param jSoupConfig
	 * @return The text (without HTML code), or the HTML representation if getHTML is true, from the selected element
	 */
	@Override
	String getJsoupElement(Document jsoupDoc, Map<String, String> jSoupConfig) {
		String selector = jSoupConfig.get("selector");
		if(jsoupDoc.select(selector).size() > 0) {
			Element element = jsoupDoc.select(selector).first();
			if (returnHTML) {
				return element.outerHtml().replaceAll(HTML_OUTPUT_CLEANUP, "");
			} else {
				return element.text();
			}
		}
		else {
			return "";			
		}
	}
	
	public boolean getReturnHTML() {
		return this.returnHTML;
	}
	
	public void setReturnHTML(boolean b) {
		this.returnHTML = b;
	}
}
