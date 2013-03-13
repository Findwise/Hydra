package com.findwise.hydra.stage;

import net.htmlparser.jericho.*;
import com.findwise.hydra.local.LocalDocument;
import java.util.List;

/**
 * @author Roar Granevang
 */
@Stage(description = "This stage render a html page as text, removing all html tags, css styling and javascript")
public class RenderHTMLStage extends AbstractProcessStage {

	@Parameter(name = "fields", description = "The fields containing the html text to be rendered(removing html and such)")
	private List<String> fields;

	@Override
	public void init() throws RequiredArgumentMissingException {
		if (fields == null || fields.isEmpty()) {
			throw new RequiredArgumentMissingException("fields is missing. Need to know what fields to render");
		}
	}

	@Override
	public void process(LocalDocument doc) throws ProcessException {

		for (String field : fields) {
			String stringToRender = (String) doc.getContentField(field);
			if (stringToRender == null || stringToRender.isEmpty()) {
				continue;
			} else {
				Source source = new Source(stringToRender);
				String renderedString = source.getRenderer().toString();
				doc.putContentField(field, renderedString);
			}
		}
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}
}
