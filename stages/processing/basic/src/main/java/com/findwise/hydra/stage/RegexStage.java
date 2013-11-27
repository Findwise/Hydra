package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration string in JSON format:
 * [{inField:rawcontent,outField:out,regex:\
 * "(^[A-Za-z0-9]*.[A-Za-z0-9]*).*\",substitute:\"$1.ru\"}]
 *
 * It can also contain multiple configurations:
 * [{inField:fieldname,outField:fieldname
 * ,regex:regularexpression,substitute:$x},
 * {inField:fieldname,outField:fieldname,
 * regex:regularexpression,substitute:$x}]:
 *
 * inField: The field the regex should be performed on outField: The field the
 * result from the regex stage should output to regex: The regular expression,
 * backslashes needs to be double escaped substitute: The group being replaced
 * where the group number is prefixed with $
 *
 * @author jens.bengtsson
 * @author leonard.saers
 * @author kristoffer.vinell
 *
 */
@Stage(description = "Regular expression matching on a field, where a substitute regular expression is put in the output field. Backslashes need to be double escaped!")
public class RegexStage extends AbstractProcessStage {
    private static Logger logger = LoggerFactory.getLogger(RegexStage.class);

    @Parameter(name = "regexConfigs", required = true,
    		description = "List of configs, where each config is a map with the keys " +
    				"'regex', 'inField' and 'outField' and 'substitute'")
    private List<Map<String, String>> regexConfigs;
    @Parameter(name = "concatenateMatches",
    		description = "Will concatenate the extracted matches for the input string(s), enabled by default.")
    private boolean concatenateMatches = true;
    @Parameter(name = "concatenateListElements",
    		description = "Will add matches from all the strings in the input list to a single list, enabled by default.")
    private boolean concatenateListElements = true;

    @Override
    public void init() throws RequiredArgumentMissingException {
        if (this.regexConfigs == null || this.regexConfigs.size() == 0) {
            throw new RequiredArgumentMissingException("regexConf was probably not parsed correctly");
        }
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        for (Map<String, String> regexConf : regexConfigs) {
            Pattern pattern = Pattern.compile(regexConf.get("regex"), Pattern.DOTALL);
            Object value = doc.getContentField(regexConf.get("inField"));
            if (value == null) {
                continue;
            }
            List<List<String>> outData = new ArrayList<List<String>>();
            if (value instanceof String) {
                String input = (String) doc.getContentField(regexConf.get("inField"));
                processStringInput(regexConf, pattern, outData, input);
            } else if (value instanceof List<?>) {
                for (Object listValue : (List<?>) value) {
                    if (listValue instanceof String) {
                        processStringInput(regexConf, pattern, outData, (String) listValue);
                    } else {
                        logger.info("List did not contain all Strings");
                    }
                }
            } else {
                logger.info("Field type of inField was not recognized. Valid field types are String and List<String>");
                continue;
            }
            addMatchesToDocument(doc, regexConf, outData);
        }
    }

	private void processStringInput(Map<String, String> regexConf, Pattern pattern,
			List<List<String>> outData, String input) {
		Matcher regexMatcher = pattern.matcher(input);
		List<String> extractedMatches = extractMatches(regexConf, regexMatcher);
		if (!extractedMatches.isEmpty()) {
			outData.add(extractedMatches);
		}
	}

	private List<String> extractMatches(Map<String, String> regexConf, Matcher regexMatcher) {
    	List<String> extractions = new ArrayList<String>();
    	while (regexMatcher.find()) {
			StringBuilder sb = new StringBuilder(regexConf.get("substitute"));
			int index = sb.indexOf("$");
			while (index != -1) {
				char group = sb.charAt(index + 1);
				int groupNr = group - '0';
				if (groupNr >= 0 && groupNr <= regexMatcher.groupCount()) {
					String match = regexMatcher.group(groupNr);
					sb.replace(index, index + 2, match);
					index += match.length();
					index = sb.indexOf("$", index);
				}
			}
			extractions.add(sb.toString());
		}
    	return extractions;
    }

	private void addMatchesToDocument(LocalDocument doc, Map<String, String> regexConf,
			List<List<String>> outData) {
		if (outData.isEmpty()) {
			return;
		}
		if (concatenateListElements) {
			List<String> concatenated = concatenateLists(outData);
			if (concatenateMatches) {
				doc.putContentField(regexConf.get("outField"), concatenateStrings(concatenated));
			} else {
				doc.putContentField(regexConf.get("outField"), concatenated);
			}
		} else {
			if (concatenateMatches) {
				doc.putContentField(regexConf.get("outField"), concatenateMatches(outData));
			} else {
				doc.putContentField(regexConf.get("outField"), outData);
			}
		}
	}

	private List<String> concatenateLists(List<List<String>> lists) {
		List<String> ret = new ArrayList<String>();
		for (int i = 0; i < lists.size(); i++) {
			ret.addAll(lists.get(i));
		}
		return ret;
	}

    private String concatenateStrings(List<String> strings) {
    	StringBuilder concatenated = new StringBuilder();
    	for (String s : strings) {
    		concatenated.append(s);
    	}
		return concatenated.toString();
	}

	private List<String> concatenateMatches(List<List<String>> outData) {
		List<String> ret = new ArrayList<String>();
		for (List<String> list : outData) {
			ret.add(concatenateStrings(list));
		}
		return ret;
	}
}
