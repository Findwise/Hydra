package com.findwise.hydra.debugging;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.local.Local;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * This stage will print any files to a specified folder (intended for debugging)
 * 
 * The config is as follows:
 * 
 * path:"c:/findwise/"
 * prefix:"true"
 * 
 * @author Sture Svensson
 */
@Stage(description = "This stage will copy the first field found in the specifed order.")
public class FilesToDiskDumper extends AbstractProcessStage {

    @Parameter(name = "path", description = "Dump the files to this path")
    private String path;
    @Parameter(name = "prefixWithId", description = "If the stage sould prefix the \"filenames\" with id of the item")
    private Boolean prefix = false;

    @Override
    public void init() throws RequiredArgumentMissingException {
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {

        List<String> files;
        try {
            files = getRemotePipeline().getFileNames(doc.getID());

            for (String fileName : files) {
                DocumentFile<Local> df = getRemotePipeline().getFile(fileName, doc.getID());

                File dir = new File(path + "/");
                dir.mkdirs();
                
                File file = null;
                if (prefix) {
                    file = new File(path + "/" + doc.getContentField("id") + "_" + fileName);
                } else {
                    file = new File(path + "/" + fileName);
                }
                
                file.createNewFile();
                FileOutputStream fw = new FileOutputStream(file);
                InputStream in = df.getStream();


                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    fw.write(buffer, 0, len);
                }
                fw.close();
            }
        } catch (IOException ex) {
            throw new ProcessException("", ex);
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Boolean getPrefix() {
        return prefix;
    }

    public void setPrefix(Boolean prefix) {
        this.prefix = prefix;
    }
    
    
}
