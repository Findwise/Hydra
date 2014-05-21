package com.findwise.utils.tika;

import java.util.List;

/**
 * Sanitizer for content extracted by Tika
 *
 * In some cases, Tika will generate null and other control characters
 * that can cause problems when used throughout Hydra.
 */
public class TextSanitizer {

    /**
     * Filters invalid characters
     */
    public String filterInvalidChars(String s) {
        if (s == null) return null;

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (isValidChar(c)) {
                result.append(c);
            }
        }

        return result.toString();
    }

    private boolean isValidChar(char c) {
        return Character.isDefined(c) && c != '\uFFFD' && c != '\u0000';
    }

    /**
     * Modifies list in place, but also returns it for convenience.
     */
    public List<String> filterInvalidChars(List<String> list) {
        for(int i=0; i<list.size(); i++) {
            list.set(i, filterInvalidChars(list.get(i)));
        }
        return list;
    }
}
