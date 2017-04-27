package com.yuanyu;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.log4j.Logger;


public class UpdateViewSchema {
	private static Logger logger = Logger.getLogger(UpdateViewSchema.class);
	private static final String TABLE_ID = "table_id";
	private static final String TABLE_USER_NAME = "table_userName";
	private static final String TABLE_USER_PHONE = "table_userPhone";
	private static final String TABLE_USER_EMAIL = "table_userEmail";
	private static final String TABLE_PRIVILEGES = "table_privileges";
	private static final String JOIN_COND = "join_cond";
	private static final String CONNECT_STR = "connect_str";
	private static final String DriverName = "driverName";
	private static final String TABLE_DETAIL = "table_detail";
	private static final String TABLE_T_PRIVILEGES = "table_t_privileges";
	private static final String UpdateSchemePrefix= "CREATE OR REPLACE VIEW dashboard_user AS ";
	private static Properties prop = new Properties();
	static {
		loadProperties();
	}
	
	public static void loadProperties(){
		InputStream input = null;
	    try {
			input = new FileInputStream("./config/schema.properties");
			prop.load(input);
			logger.info("TABLE_ID:"+ prop.getProperty(TABLE_ID));
			logger.info("TABLE_USER_NAME :" + prop.getProperty(TABLE_USER_NAME));
			logger.info("TABLE_USER_PHONE: " + prop.getProperty(TABLE_USER_PHONE ));
			logger.info("TABLE_USER_EMAIL: " + prop.getProperty(TABLE_USER_EMAIL));
		    logger.info("TABLE_PRIVILEGES: " + prop.getProperty(TABLE_PRIVILEGES));
		    logger.info("JOIN_COND: " + prop.getProperty(JOIN_COND));
		    logger.info("CONNECT_STR: " + prop.getProperty(CONNECT_STR));
		    logger.info("DriverName: " + prop.getProperty(DriverName));
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally{
			if(input != null){
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
			logger.info("properties loaded!!");
		}
	}
	public static void main(String[] args) {
		Connection conn = null;
		try {
			
			Class c = Class.forName(prop.getProperty(DriverName));
			conn = DriverManager.getConnection(prop.getProperty(CONNECT_STR));
			Statement stmt = conn.createStatement();
			StringBuffer sb = new StringBuffer(UpdateSchemePrefix);
			sb.append("SELECT ");
			sb.append(prop.getProperty(TABLE_ID)+",");
			sb.append(prop.getProperty(TABLE_USER_EMAIL)+",");
			sb.append(prop.getProperty(TABLE_USER_NAME)+",");
			sb.append(prop.getProperty(TABLE_USER_PHONE)+",");
			sb.append(prop.getProperty(TABLE_PRIVILEGES)+" FROM "+prop.getProperty(TABLE_DETAIL)+","+ prop.getProperty(TABLE_T_PRIVILEGES));
			sb.append(" where "+ prop.getProperty(JOIN_COND));
			stmt.executeUpdate(sb.toString());
			
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

	}

}
