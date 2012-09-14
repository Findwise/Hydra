/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import java.util.LinkedList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;

/**
 *
 * @author Roar Granevang
 */
public class PopulateQueryCompletionFieldsTest {

    public PopulateQueryCompletionFieldsTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Test(expected = RequiredArgumentMissingException.class)
    public void initFailCollecitonNameMissing() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        List collectonList = new LinkedList<String>();
        collectonList.add("gard");
        collectonList.add("bridge");
        doc.putContentField("collection", collectonList);
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");

        stage.setCopyFields(copyFields);
        stage.init();

        assertEquals("Battleships", doc.getContentField("gard_qc"));
        assertEquals("Battleships", doc.getContentField("bridge_qc"));

    }

    @Test
    public void testProcessBothGardAndBridge() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        List collectonList = new LinkedList<String>();
        collectonList.add("gard");
        collectonList.add("bridge");
        doc.putContentField("collection", collectonList);
        stage.setCollectionName("collection");
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");

        stage.setCopyFields(copyFields);
        stage.process(doc);

        assertEquals("Battleships", doc.getContentField("gard_qc"));
        assertEquals("Battleships", doc.getContentField("bridge_qc"));
    }

    @Test
    public void testProcessBothGardAndBridgeWith2copyFields() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        doc.putContentField("subtitle", "Big ships");
        List collectonList = new LinkedList<String>();
        collectonList.add("gard");
        collectonList.add("bridge");
        doc.putContentField("collection", collectonList);
        stage.setCollectionName("collection");
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");
        copyFields.add("subtitle");

        stage.setCopyFields(copyFields);
        stage.process(doc);

        List<String> expectedList = new LinkedList<String>();
        expectedList.add("Battleships");
        expectedList.add("Big ships");
        assertEquals(expectedList, doc.getContentField("gard_qc"));
        assertEquals(expectedList, doc.getContentField("bridge_qc"));
    }

    @Test
    public void testProcessOnlyBridge() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        doc.putContentField("collection", "bridge");
        stage.setCollectionName("collection");
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");

        stage.setCopyFields(copyFields);
        stage.process(doc);

        assertEquals("Battleships", doc.getContentField("bridge_qc"));
    }

    @Test
    public void testProcessOnlyBridgeWith2copyFields() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        doc.putContentField("subtitle", "Big ships");
        List collectonList = new LinkedList<String>();
        collectonList.add("bridge");
        doc.putContentField("collection", collectonList);
        stage.setCollectionName("collection");
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");
        copyFields.add("subtitle");

        stage.setCopyFields(copyFields);
        stage.process(doc);

        List<String> expectedList = new LinkedList<String>();
        expectedList.add("Battleships");
        expectedList.add("Big ships");
        assertEquals(expectedList, doc.getContentField("bridge_qc"));
    }

    @Test
    public void testStaticMapping() throws Exception {
        PopulateQueryCompletionFields stage = new PopulateQueryCompletionFields();

        LocalDocument doc = new LocalDocument();
        doc.putContentField("title", "Battleships");
        doc.putContentField("type", "AAA");
        stage.setCollectionName("type");
        List<String> copyFields = new LinkedList<String>();
        copyFields.add("title");
        stage.setCopyFields(copyFields);
        
        List<String> statics = new LinkedList<String>();
        statics.add("AAA:news");
        stage.setStaticMappingList(statics);
        stage.init();

        stage.process(doc);

        assertEquals("Battleships", doc.getContentField("news_qc"));
    }
}
