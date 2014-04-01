package com.findwise.utils.tika;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class InputStreamParser {

    private final Parser parser;

    public InputStreamParser() {
        parser = new AutoDetectParser();
    }

    public InputStreamParser(Parser parser) {
        this.parser = parser;
    }

    public ParsedData parse(InputStream stream) throws TikaException, SAXException, IOException {
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        parseContext.set(Parser.class, parser);
        StringWriter textData = new StringWriter();
        parser.parse(stream, new BodyContentHandler(textData), metadata, parseContext);
        String content = textData.toString();

        return new ParsedData(content, metadata);
    }
}
