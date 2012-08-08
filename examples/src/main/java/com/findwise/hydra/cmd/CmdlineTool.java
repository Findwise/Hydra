package com.findwise.hydra.cmd;

import com.beust.jcommander.JCommander;
import com.findwise.hydra.MongoDBConnectionConfig;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.json.JsonPipelineUtil;
import com.findwise.hydra.json.PipelineConfiguration;
import com.findwise.hydra.json.StageFactory;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.Guice;
import com.google.inject.Module;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iool for uploading jar files or stages to mongodb. This can be
 * invoked either regular java class or as a commandline tool. 
 * 
 * @author johan.sjoberg
 */
public class CmdlineTool {

    private static final Logger log = LoggerFactory.getLogger(CmdlineTool.class);
    private final JsonPipelineUtil jsonReader = new JsonPipelineUtil();
    private final StageFactory stageFactory = new StageFactory();

    /**
     * <pre>
     * Usage: 
     *     &lt;cmd&gt; &lt;opts&gt;
     * 
     *     upload -jar &lt;jarFile&gt; &lt;pipelineName&gt; &lt;jarId&gt;
     *     upload -c &lt;pipelineName.json&gt;
     * 
     * <tt>pipelineName</tt> is the name of the pipeline to upload the jar file to. 
     * <tt>jarId</tt> is the identifier stages use to reference the jar file. 
     * 
     * Commands:
     *    upload 
     *          upload a file or configuration to mongodb
     * 
     * Options: 
     *    -c --config
     *          the pipeline configuration to upload
     * 
     *    -s --stage
     *          name of a specific stage in the pipeline to upload
     *          Only makes sence to use when uploading a config "-c". 
     * 
     *    -f --jarfile 
     *          the jar file to upload 
     * 
     *    -h --host
     *          name of the host running mongodb. Defaults to localhost.
     * 
     * Sample usage:
     *    upload -f myLib.jar the-pipeline myLib
     *    upload -c hydra-pipeline.json
     * 
     * </pre>
     * @param args 
     */
    public static void main(String[] args) throws IOException {
        final UploadCommand upload = new UploadCommand();
        final JCommander jc = new JCommander();
        jc.addCommand("upload", upload);
        jc.parse(args);
        new CmdlineTool().performCommand(jc, upload);
    }

    /**
     * Performs the upload command
     */
    public void performCommand(JCommander jc, UploadCommand cmd) throws IOException {
        log.info(cmd.toString());
        if (!cmd.isValid() || cmd.isHelp()) {
            jc.usage("upload");
            return;
        }

        if (cmd.getJarFile() != null) {
            File f = new File(cmd.getJarFile().getFilename());

            Module conf = new MongoDBConnectionConfig(cmd.getJarFile().getPipelinename(), cmd.getHost());
            MongoConnector mdc = Guice.createInjector(conf).getInstance(MongoConnector.class);
            mdc.connect();

            log.info("Uploading jar file");
            File file = new File(cmd.getJarFile().getFilename());
            mdc.getPipelineWriter().save(cmd.getJarFile().getId(), file.getName(), new FileInputStream(f));
        }

        if (cmd.getConfig() != null) {
            PipelineConfiguration pipelineConfig = jsonReader.fromJson(cmd.getConfig());
            List<Stage> stages = stageFactory.createStages(pipelineConfig);

            Module conf = new MongoDBConnectionConfig(pipelineConfig.getPipelineName(), cmd.getHost());
            MongoConnector mdc = Guice.createInjector(conf).getInstance(MongoConnector.class);
            mdc.connect();

            Pipeline<Stage> pipeline = mdc.getPipelineReader().getPipeline();
            for (Stage stage : stages) {
                if (cmd.getStageNames() != null) {
                    if (cmd.getStageNames().contains(stage.getName())) {
                        log.info("Preparing to upload stage, " + stage.getName());
                        pipeline.addStage(stage);
                    }
                } else {
                    log.info("Preparing to upload stage, " + stage.getName());
                    pipeline.addStage(stage);
                }
            }
            log.info("Uploading stages");
            log.info(pipeline.toString());
            mdc.getPipelineWriter().write(pipeline);
        }
    }
}