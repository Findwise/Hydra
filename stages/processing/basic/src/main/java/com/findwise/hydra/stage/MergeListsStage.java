package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stage(description = "Merges lists into a single list of tuples, where each item is a tuple " +
		"of the corresponding items in the input lists, separated by a separator. " +
		"This stage is incredibly specific in it's usecase, and most likely not the one you want, " +
		"since it merges lists into a list of tuples, rather than merging two lists." +
		"See MergeFieldsStage for more generic field merging capabilities.")
public class MergeListsStage extends AbstractProcessStage {
    private Logger logger = LoggerFactory.getLogger(MergeListsStage.class);

	@Parameter(name = "inFields", required = true, description = "List of fields to use")
	protected List<String> inFields;
	
	@Parameter(name = "outField", required = true, description = "Field to put the output list in")
	protected String outField;
	
	@Parameter(name = "separator", required = true, description = "Separator for separating the concatenated items")
	protected String separator;
	
	
	@Override
	public void process(LocalDocument doc) {
		
		List<String> outList = new ArrayList<String>();
		List<List<String>> inLists = new ArrayList<List<String>>();
		int length = -1;
		for (String field : inFields) {
			Object fieldContent = doc.getContentField(field);
			if (fieldContent == null) {
				logger.info("Field " + field + " does not exist, skipping document");
				return;
			}
			if (fieldContent instanceof List<?>) {
				@SuppressWarnings("unchecked")
				List<String> list = (List<String>)fieldContent;
				if (length == -1) length = list.size();
				
				if (list.size() == length) {
					inLists.add(list);
				}
				else {
					// Lists have different lengths
					logger.info("Lists in inFields are of different lengths, skipping document");
					return;
				}
			}
			else {
				// Field is not a list
				logger.info("Field " + field + " is not a list, skipping document");
				return;
			}
		}
		for (int item = 0; item < length; item++) {
			StringBuilder strb = new StringBuilder();
			for (int list = 0; list < inLists.size(); list++) {
				if (list > 0) strb.append(separator);
				strb.append(inLists.get(list).get(item));
			}
			outList.add(strb.toString());
		}
		doc.putContentField(outField, outList);
	}

	@Override
	public void init() throws RequiredArgumentMissingException { }

}
