package com.findwise.hydra.stage.groovyrunner;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.InitFailedException;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

@Stage(description = "Runs a simple Groovy script, that has access to the document being processed")
public class GroovyShellStage extends AbstractProcessStage {

    @Parameter(name = "scriptText", description = "Groovy script to execute. Make sure you add newlines or end your lines with semicolons!")
    private String scriptText = "";

    private Script script;

    @Override
    public void init() throws RequiredArgumentMissingException, InitFailedException {
        GroovyShell groovyShell = new GroovyShell();
        script = groovyShell.parse(scriptText);
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        Binding binding = new Binding();
        binding.setVariable("doc", doc);
        script.setBinding(binding);
        script.run();
    }

    public String getScriptText() {
        return scriptText;
    }

    public void setScriptText(String scriptText) {
        this.scriptText = scriptText;
    }
}
