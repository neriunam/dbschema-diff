package demo.schemacrawler;

import java.util.Map;

import lombok.Data;

@Data
public class Table {
	
	private String tableName;
	
	private Map<String, String> columns;
	
}
