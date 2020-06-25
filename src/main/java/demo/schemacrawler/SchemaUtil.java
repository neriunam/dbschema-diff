package demo.schemacrawler;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.LoadOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.utility.SchemaCrawlerUtility;

@Slf4j
public class SchemaUtil {
		
	public void compare(Connection connection, Properties prop) throws Exception {
		log.info("Compare json schema vs database schema");
		ObjectMapper mapper = new ObjectMapper();
		File file = new File(prop.getProperty(Constants.IN_JSON_SCHEMA_FILE));
		
		List<Table> tablesReference = mapper.readValue(file, new TypeReference<List<Table>>(){});
		List<Table> tablesDatabase =  this.getTablesFromDB(connection, prop);
		
		for (Table tableref : tablesReference) {
			log.info("Check table[{}]", tableref.getTableName());
			checkTable(tableref, tablesDatabase);
		}
	}
	
	private void checkTable(Table tableref, List<Table> tablesDatabase) throws Exception {
		//log.info("Check table {}", tableref.getTableName());
		boolean tableExist = false;
		for (Table tabledb : tablesDatabase) {
			tableExist = tableref.getTableName().equalsIgnoreCase(tabledb.getTableName());
			//log.info("{} compare {}", tableref.getTableName(), tabledb.getTableName());
			if (tableExist) {
				//log.info("Table exist");
				checkColumn(tableref, tabledb);
				break;
			}
		}
		if (!tableExist) {
			throw new Exception("Table[" + tableref.getTableName() + "] not found in database");
		}
	}
	
	private void checkColumn(Table tableref, Table tabledb) throws Exception {
		for (Entry<String, String> entry : tableref.getColumns().entrySet()) {
			//log.info("columnref: {}, typeref: {}", entry.getKey(), entry.getValue());
			String type = tabledb.getColumns().get(entry.getKey());
			if (type == null) {
				throw new Exception("Column[" + entry.getKey() + "] in Table[" + tableref.getTableName() + "] not found in database");
			}
			else {
				//log.info("columndb: {}, typerdb: {}", entry.getKey(), type);
				if (!type.equalsIgnoreCase(entry.getValue())) {
					throw new Exception("Column[" + entry.getKey() + "] in Table[" + tableref.getTableName() + "]: type must be [" + entry.getValue() + "] but found type [" + type  + "]");
				}
			}
		}
	}
	
	public void createJsonFileTables(Connection connection, Properties prop) throws Exception {
		File file = new File(prop.getProperty(Constants.OUT_JSON_SCHEMA_FILE));
		List<Table> tables = getTablesFromDB(connection, prop);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writerWithDefaultPrettyPrinter().writeValue(file, tables);
	}
	
	
	private List<Table> getTablesFromDB(Connection connection, Properties prop) throws Exception {
		String includeTables = prop.getProperty(Constants.INCLUDE_TABLES);
		if (includeTables == null) { includeTables = ""; }
		final LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder()
				.includeSchemas(new RegularExpressionInclusionRule(prop.getProperty(Constants.SCHEMA)));
		if (includeTables.length() > 0) {
			String[] includes = prop.getProperty(Constants.INCLUDE_TABLES).split(",");
			StringBuilder sb = new StringBuilder();
			for (String s : includes) {
				sb.append(prop.getProperty(Constants.SCHEMA)).append(".").append(s.trim()).append("|");
			}
			log.info("Include tables: {}", sb.toString());
			limitOptionsBuilder.includeTables(new RegularExpressionInclusionRule(sb.toString()));
		}
		
		final LoadOptionsBuilder loadOptionsBuilder = LoadOptionsBuilder.builder()
				.withSchemaInfoLevel(SchemaInfoLevelBuilder.standard());
	    
	    final SchemaCrawlerOptionsBuilder optionsBuilder = SchemaCrawlerOptionsBuilder
	        .builder()
	        .withLoadOptionsBuilder(loadOptionsBuilder)
	        .withLimitOptionsBuilder(limitOptionsBuilder);
	    
	    final SchemaCrawlerOptions options = optionsBuilder.toOptions();
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);
		
		List<Table> tables = new ArrayList<>();
		Table mytable;
		
		StringBuilder sb = new StringBuilder("[");
		for (final Schema schema : catalog.getSchemas()) {
			System.out.println(schema);
			for (final schemacrawler.schema.Table table : catalog.getTables(schema)) {
				mytable = new Table();
				sb.append(table.getFullName()).append(",");
				//log.info("Table: {}", table.getFullName());
				mytable.setTableName(table.getName());
				mytable.setColumns(new LinkedHashMap<String, String>());
				for (final Column column : table.getColumns()) {
					mytable.getColumns().put(column.getName(), column.getType().getName());
				}
				tables.add(mytable);
			}
		}
		if (sb.length() > 1) {
			sb.delete(sb.length()-1, sb.length());
		}
		sb.append("]");
		log.info("Tables loaded from database: {}", sb.toString());
		return tables;
	}	
	
}
