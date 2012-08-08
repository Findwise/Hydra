package com.findwise.hydra.cmd;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.MongoDBConnectionConfig;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.Guice;
import com.google.inject.Module;
import java.io.*;

/**
 * Tool to upload jar-files to hydra
 * ==========================================================
 * This is currently not used but remains here as reference. 
 * ==========================================================
 */
public class CmdlineInserter {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
            return;
        }
        System.out.println("Attempting to work with pipeline '" + args[2] + "'");
        Module conf = new MongoDBConnectionConfig(args[2], "127.0.0.1");
        MongoConnector mdc = Guice.createInjector(conf).getInstance(MongoConnector.class);

        mdc.connect();

        if (args[0].equals("add")) {
            if (args[1].equals("library")) {
                Object outId = addFile(mdc, args[3]);
                if (outId != null) {
                    System.out.println("Added stage library with id: " + outId);
                }
            } else if (args[1].equals("stage")) {
                String name = args[3];
                String libraryId = args[4];
                Map<String, Object> map;
                if (args.length < 6) {
                    map = readPropertiesFile(name);
                } else {
                    String maps = StringUtils.join(Arrays.copyOfRange(args, 5, args.length), " ");
                    map = SerializationUtils.fromJson(maps);
                }
                DatabaseFile df = new DatabaseFile();
                df.setId(new ObjectId(libraryId));
                Stage s = new Stage(name, df);
                s.setProperties(map);
                Pipeline<Stage> pipeline = mdc.getPipelineReader().getPipeline();
                pipeline.addStage(s);
                mdc.getPipelineWriter().write(pipeline);
                System.out.println("Added stage " + name + " to the pipeline. There are now " + pipeline.getStages().size() + " stages in the pipeline");
            }
        }
    }

    private static void printUsage() {
        System.out.println("Possible arguments of this class:\n");
        System.out.println("add library <pipeline-name> <your-stage-library>.jar\n - adds xyz.jar as a stage library in Hydra. Returns unique ID of this library");
        System.out.println("add stage <pipeline-name> <stage-name> <library-unique-id> <stage-arguments-json>\n - configures a stage into the Hydra, directing Hydra to look for the stage class in the library with the unique id <library-unique-id>");
        System.out.println("--- alternative usage with property files ---");
        System.out.println("add stage <pipeline-name> <stage-name> <library-unique-id>\n - configures a stage into the Hydra, directing Hydra to look for the stage class in the library with the unique id <library-unique-id>. Assumes settings are written in <stage-name>.properties file");
    }

    private static Map<String, Object> readPropertiesFile(String stageName) {
        String json = "";
        try {
            json = readFileAsString(stageName + ".properties");
        } catch (IOException e) {
            System.err.println("Property file " + stageName + ".properties could not be read");
            System.exit(-1);
        }
        try {
            return SerializationUtils.fromJson(json);
        } catch (JsonException e) {
            System.err.println("Property file " + stageName + ".properties is not well formed json");
            System.exit(-1);
        }
        return null;
    }

    private static String readFileAsString(String filePath)
            throws java.io.IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }

    public static Object addFile(MongoConnector dbc, String jar) throws FileNotFoundException, URISyntaxException {
        URL path = ClassLoader.getSystemResource(jar);
        if (path == null) {
            System.out.println("Unable to locate file " + jar);
            return null;
        }
        File f = new File(path.toURI());
        return dbc.getPipelineWriter().save(f.getName(), new FileInputStream(f));
    }
}