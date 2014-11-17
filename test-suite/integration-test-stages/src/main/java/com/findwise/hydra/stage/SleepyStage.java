package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Stage
public class SleepyStage extends AbstractProcessStage {
    private Random random = new Random();

    @Parameter(required = true, name = "fieldValueMap", description = "A map of fields to modify, and the values to write to them")
    private Map<String, Object> fieldValueMap;

    @Parameter(name="baseSleep")
    private int baseSleep = 50;

    @Parameter(name="variantSleep")
    private int variantSleep = 1000;

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        int timeout = random.nextInt(variantSleep) + baseSleep;
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            return;
        }

        for (Map.Entry<String, Object> entry : fieldValueMap.entrySet()) {
            final String key = entry.getKey();
            doc.putContentField(key, entry.getValue());
        }
    }
}
