package com.findwise.hydra.stage;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discards documents in Hydra depending on regex matched on a specified field.
 * 
 * The stage takes its configuration from a properties-file and needs the following arguments:
 * discardConfigs=[{field:<meta-data-field-name>, regex:<regex>}]
 * 
 * where the right-hand side is a configuration string in JSON format where
 * 
 * <meta-data-field-name>: The field the regex should be performed on
 * <regex>: The regular expression, backslashes needs to be double escaped
 * 
 * It can also contain multiple configurations:
 * [{field:<meta-data-field-name1>, regex:<regex1>}, {field:<meta-data-field-name2>, regex:<regex2>}, {field:<meta-data-field-name3>, regex:<regex3>}]
 * 
 *
 * Example: [{field:url, regex:\".*[.]com.*\"}, {field:url, regex:\".*[.]se.*\"}, {field:url, regex:\".*[.]org.*\"}]
 * 
 * @author kristoffer.vinell
 * @author leonard.saers
 * 
 */
@Stage
public class DocumentDiscardStage extends AbstractProcessStage {
    private Logger logger = LoggerFactory.getLogger(DocumentDiscardStage.class);

	@Parameter(required = true, description = "Mapping between fields and regexes")
	private List<Map<String, String>> discardConfigs;

	public List<Map<String, String>> getDiscardConfigs() {
		return discardConfigs;
	}

	public void setDiscardConfigs(List<Map<String, String>> discardConfigs) {
		this.discardConfigs = discardConfigs;
	}

	@Override
	public void init() throws RequiredArgumentMissingException {

		if (this.discardConfigs == null || this.discardConfigs.size() == 0) {
			throw new RequiredArgumentMissingException("regexConf was probably not parsed correctly");
		}
		
	}
	
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		for (Map<String, String> discardConfig : discardConfigs) {
			if (doc.isDiscarded()) {
				return;
			}
			if (shouldBeDiscarded(doc, discardConfig)) {
				try {
					doc.discard();
				} catch (Exception e) {
					throw new ProcessException("Couldn't mark document with id: " + doc.getID().toJSON() + " as discarded.");
				}
			}
		}
	}
	
	private boolean shouldBeDiscarded(LocalDocument doc, Map<String, String> discardConfig) {
		Pattern pattern = Pattern.compile(discardConfig.get("regex"), Pattern.DOTALL);
		Object value = doc.getContentField(discardConfig.get("field"));
		
		if (value instanceof String) {
			Matcher regexMatcher = pattern.matcher((String) value);
			return regexMatcher.matches();
		} else if (value instanceof List<?>) {
			for (Object val : (List<?>)value) {
				if (val instanceof String) {
					Matcher regexMatcher = pattern.matcher((String) val);
					if (regexMatcher.matches()) {
						return true;
					}
				} else {
					logger.info("Field " + discardConfig.get("field") + " was a list but not a List<String>");
					return false;
				}
			}
			return false;
		}
		logger.info("Field " + discardConfig.get("field") + " did not contain String or List<String>");
		return false;
	}
	
}
