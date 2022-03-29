package examples.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class HBaseDemo {
	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;
	public static void main(String[] args) {
		connect();
		try {
			put("Student", "2", "Name", "Scofield");
			put("Student", "2", "Score:English", "45");
			put("Student", "2", "Score:Math", "89");
			put("Student", "2", "Score:Computer", "100");
			get("Student", "2", "Score:English");
		} catch (IOException e) {
			e.printStackTrace();
		}
		disconnect();
	}

	public static void put(String tableName, String rowKey, String column, String value) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowKey.getBytes());
		String[] cols = column.split(":");
		byte[] family = cols[0].getBytes(), qualifier = (cols.length == 2) ? cols[1].getBytes() : "".getBytes();
		put.addColumn(family, qualifier, value.getBytes());
		table.put(put);
		table.close();
	}

	public static void get(String tableName, String rowKey, String column) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		String[] cols = column.split(":");
		byte[] family = cols[0].getBytes(), qualifier = (cols.length == 2) ? cols[1].getBytes() : "".getBytes();
		get.addColumn(family, qualifier);
		Result result = table.get(get);
		print(result);
	}

	public static void print(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("rowName: " + new String(CellUtil.cloneRow(cell)));
			System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
			System.out.println("timestamp: " + cell.getTimestamp());
		}
	}

	public static void connect() {
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void disconnect() {
		try {
			if (admin != null)
				admin.close();
			if (connection != null)
				connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
