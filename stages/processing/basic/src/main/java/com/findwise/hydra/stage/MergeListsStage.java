package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;

@Stage(description = "Merges lists into a single list, where each item is a concatenation of the corresponding items in the input lists, separated by a separator. " +
		"This Stage is deprecated, as all its functionality and more is implemented in com.findwise.hydra.stage.MergeFieldsStage")
@Deprecated
public class MergeListsStage extends AbstractProcessStage {

	@Parameter(name = "inFields", description = "List of fields to use")
	protected List<String> inFields;
	
	@Parameter(name = "outField", description = "Field to put the output list in")
	protected String outField;
	
	@Parameter(name = "separator", description = "Separator for separating the concatenated items")
	protected String separator;
	
	
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		
		List<String> outList = new ArrayList<String>();
		List<List<String>> inLists = new ArrayList<List<String>>();
		int length = -1;
		for (String field : inFields) {
			Object fieldContent = doc.getContentField(field);
			if (fieldContent == null) {
				Logger.warn("Field " + field + " does not exist, skipping document");
				return;
			}
			if (fieldContent instanceof List<?>) {
				List<String> list = (List<String>)fieldContent;
				if (length == -1) length = list.size();
				
				if (list.size() == length) {
					inLists.add(list);
				}
				else {
					// Lists have different lengths
					Logger.warn("Lists in inFields are of different lengths, skipping document");
					return;
				}
			}
			else {
				// Field is not a list
				Logger.warn("Field " + field + " is not a list, skipping document");
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
