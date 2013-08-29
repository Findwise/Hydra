package com.findwise.hydra.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonIOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;

/**
 * A tool to read and produce json from, and to a {@link PipelineConfiguration}. 
 * 
 * @author johan.sjoberg
 */
public class JsonPipelineUtil {

    private final Gson gson;

    /**
     * Creates a new instance
     */
    public JsonPipelineUtil() {
        gson = new GsonBuilder()
                .registerTypeAdapter(Object.class, new PipelineConfigurationDeserializer())
                .create();
    }

    /**
     * Parse a json configuration file into a PipelineConfiguration
     * 
     * @param reader reader to the file to parse
     * @return Corresponding <tt>PipelineConfiguration</tt>
     * @throws JsonSyntaxException if json is not a valid representation for a <tt>PipelineConfiguration</tt>
     */
    public PipelineConfiguration fromJson(InputStreamReader reader) {
        PipelineConfiguration pipeline = gson.fromJson(reader, PipelineConfiguration.class);
        return pipeline;
    }

    /**
     * Parse a json configuration file into a PipelineConfiguration
     * 
     * @param fileName full name, including path, of the file to parse
     * @return Corresponding <tt>PipelineConfiguration</tt>
     * @throws JsonSyntaxException if json is not a valid representation for a <tt>PipelineConfiguration</tt>
     */
    public PipelineConfiguration fromJson(String fileName) throws FileNotFoundException {
        return fromJson(new FileReader(fileName));
    }

    /**
     * Serialize a PipelineConfiguration to json
     * 
     * @param pipeline the pipeline to serialize
     * @return the json representation of the pipeline
     * @throws JsonIOException if there was a problem writing to the writer
     */
    public String toJson(PipelineConfiguration pipeline) {
        return gson.toJson(pipeline);
    }
}
