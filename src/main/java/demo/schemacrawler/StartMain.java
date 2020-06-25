package demo.schemacrawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartMain {
	
	public static final String PROPERTIES_FILE = "/application.properties";

	public static void main(String[] args) throws Exception{
		log.info("Start main");
		new StartMain().start();
	}

	private void start() throws Exception {
		Properties prop = new Properties();
		prop.load(StartMain.class.getResourceAsStream(PROPERTIES_FILE));
		log.info("Properties: {}", prop);
		try (Connection connection = getConnection(prop)) {
			SchemaUtil schemaUtil = new SchemaUtil();
			schemaUtil.createJsonFileTables(connection, prop);
			schemaUtil.compare(connection, prop);
		} catch (Exception e) {
			log.error("Error: ", e);
		}
	}
	
	
	private Connection getConnection(Properties prop) throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put(Constants.USER_PARAM, prop.get(Constants.USER_DB));

		String url = prop.getProperty(Constants.URL_DB);

		Connection conn = DriverManager.getConnection(url, connectionProps);
		conn = DriverManager.getConnection(url, connectionProps);

		log.info("Connected to database");

		return conn;
	}
	
}
