package com.findwise.hydra.net;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
import com.findwise.hydra.stage.GroupStarter;

public class PropertiesHandlerTest {
	@SuppressWarnings("rawtypes")
	private DatabaseConnector dbc;
	private PipelineReader reader;
	private RESTServer server;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		reader = mock(PipelineReader.class);
		dbc = mock(DatabaseConnector.class);
		when(dbc.getPipelineReader()).thenReturn(reader);
		server = RESTServer.getNewStartedRESTServer(14000, new HttpRESTHandler<DatabaseType>(dbc));
	}
	
	@Test
	public void testGetStages() throws Exception {
		Pipeline p = new Pipeline();
		StageGroup g = new StageGroup("1");
		g.addStage(new Stage("stage", Mockito.mock(DatabaseFile.class)));
		g.addStage(new Stage("stage2", Mockito.mock(DatabaseFile.class)));
		p.addGroup(g);
		
		Mockito.when(reader.getPipeline()).thenReturn(p);
		
		List<String> stages = GroupStarter.getStages("localhost", server.getPort(), "1");

		assertTrue(stages.contains("stage"));
		assertTrue(stages.contains("stage2"));
		assertFalse(stages.contains("notingroup"));

		StageGroup g2 = new StageGroup("2");
		g2.addStage(new Stage("debug", Mockito.mock(DatabaseFile.class)));
		Pipeline p2 = new Pipeline();
		p2.addGroup(g2);
		
		Mockito.when(reader.getDebugPipeline()).thenReturn(p2);
		stages = GroupStarter.getStages("localhost", server.getPort(), "2");

		assertTrue(stages.contains("debug"));
		assertFalse(stages.contains("stage"));
	}

}
