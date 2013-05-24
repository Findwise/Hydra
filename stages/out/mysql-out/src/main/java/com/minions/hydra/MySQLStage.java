package com.minions.hydra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.Stage;


@Stage(description="Writes documents to a MySQL table")
public class MySQLStage extends AbstractOutputStage {
	
	@Parameter(name = "columnmap")
	private Map<String, String> columnmap;
	
	@Parameter(name = "table")
	private String table;
	
	@Parameter(name = "host")
	private String host;
	
	@Parameter(name = "db")
	private String db;
	
	@Parameter(name = "user")
	private String user;
	
	@Parameter(name = "password")
	private String password;
			
	@Override
	public void output(LocalDocument document) {
		if(columnmap.keySet().size() == 0)
		{
			Logger.error("Columnmap is empty?");
			return;
		}
		
		String[] cols = new String[columnmap.keySet().size()];
		Object[] vals = new Object[columnmap.keySet().size()];
		
		int i = 0; 
		for(String key : columnmap.keySet())
		{
			if(!document.hasContentField(key)){
				Logger.error("Document parsed does not contain field: "  + key);
				return;
			}
			
			vals[i] = document.getContentField(key);
			cols[i] = columnmap.get(key);	
			Logger.debug(key + " " + cols[i] + " " + vals[i]);
			i++;
		}			
		
		try {
			insertToDB(cols, vals);
			Logger.info("Saved row to MySQL database at " + host + "/" + db);
		} catch (SQLException e) {
			Logger.error("Failed saving to database: " + e.getMessage());
		}
	}
	
	public String createQuery(String[] cols)
	{
		if(cols.length == 0)
			throw new IllegalArgumentException("Too few columns!");
		
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + table + " SET ");
		for(String col : cols)
		{
			builder.append(" ");
			builder.append(col).append(" = ?,");
		}
		
		// Remove trailing ,
		builder.deleteCharAt(builder.length()-1);
		Logger.debug(builder.toString());
		return builder.toString();	
	}
	
	public void insertToDB(String[] cols, Object[] vals) throws SQLException
	{		
		Connection conn = DriverManager.getConnection("jdbc:mysql://"+host+"/"+db+"?user="+user+"&password="+password);
		PreparedStatement stmt = conn.prepareStatement(createQuery(cols));
		
		for(int i = 0; i < vals.length; i++)
			stmt.setObject(i+1, vals[i]);
		
		stmt.execute();		
	}

}
