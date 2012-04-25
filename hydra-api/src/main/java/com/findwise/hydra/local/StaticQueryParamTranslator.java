package com.findwise.hydra.local;

import static java.util.regex.Pattern.matches;

import java.util.Arrays;
import java.util.List;

public class StaticQueryParamTranslator implements QueryParamTranslator {

	@Override
	public LocalQuery createQueryFromString(String s) {
		return createQueryFromList(Arrays.asList(s.split(";")));

	}
	public LocalQuery createQueryFromList(List<String> properties) {
		LocalQuery ret = new LocalQuery();
		if (properties == null) {
			return ret;
		}
		for (String part : properties) {
			if (matches("contents\\(.*\\)", part)) {
				String[] splits = part.split("[\\(\\,\\)]");
				ret.getContentsExists().put(splits[1],
						Boolean.parseBoolean(splits[2]));
			}
			if (matches("touched\\(.*\\)", part)) {
				String[] splits = part.split("[\\(\\,\\)]");
				ret.getTouched()
						.put(splits[1], Boolean.parseBoolean(splits[2]));
			}
		}

		return ret;
	}

	@Override
	public String extractQueryOptions(LocalQuery q) {
		StringBuilder sb = new StringBuilder();
		for (String prop : q.getContentsExists().keySet()) {
			sb.append("contents(").append(prop).append(",")
					.append(q.getContentsExists().get(prop)).append(");");
		}
		for (String touched : q.getTouched().keySet()) {
			sb.append("touched(").append(touched).append(",")
					.append(q.getTouched().get(touched)).append(");");
		}

		return sb.toString();
	}

}
