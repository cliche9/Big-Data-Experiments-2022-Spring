package org.apache.hbase.examples;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class HBase {
	private Configuration conf;
	private Connection connection;
	private Admin admin;
	private Table table;

	public static void main(String[] args) {
		HBase hBase = new HBase();
		String opt = args[1];
		try {
			switch (opt) {
				case "create":
					hBase.createTable(args[2], Arrays.copyOfRange(args, 3, args.length));
					break;
				case "add":
					hBase.addRecord(tableName, row, fields, values);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void connect() {
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void disconnect() {
		try {
			if (admin != null)
				admin.close();
			if (connection != null)
				connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * createTable(String tableName, String[] fields)
	 * 创建表，参数tableName为表的名称，字符串数组fields为存储记录各个域名称的数组。
	 * 要求当HBase已经存在名为tableName的表的时候，先删除原有的表，然后再创建新的表。
	 */
	public void createTable(String tableName, String[] fields) throws IOException {
		connect();
		TableName tName = TableName.valueOf(tableName);
		// delete existed table
		if (admin.tableExists(tName)) {
			System.out.println("Table: " + tableName + " exists, recreating...");
			admin.disableTable(tName);
			admin.deleteTable(tName);
		}
		// create new table named 'tableName'
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
		for (String field : fields) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(field);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		admin.createTable(hTableDescriptor);
		disconnect();
	}

	/* 
	 * 向表 tableName、行 row(用 S_Name 表示)和字符串数组 fields 指定的单元格中添加对应的数据 values
	 * 其中，fields 中每个元素如果对应的列族下还有相应的列限定符的话，用 “columnFamily:column”表示
	 * 例如，同时向“Math”、“Computer Science”、“English”三列添加成绩时
	 * 字符串数组 fields 为 {“Score:Math”, ”Score:Computer Science”, ”Score:English”}
	 * 数组 values 存储这三门课的成绩
	 */
	public void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
		connect();
		Table table = connection.getTable(TableName.valueOf(tableName));
		for (int i = 0; i < fields.length; i++) {
			Put put = new Put(row.getBytes());
			String[] cols = fields[i].split(":");
			put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
			table.put(put);
		}
		table.close();
		disconnect();
	}
	/* 
	 * 浏览表 tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回 null
	 * 要求 当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据
	 * 当参数 column 为某一列具体名称(例如“Score:Math”)时，只需要列出该列的数据
	 */
	public void scanColumn(String tableName, String column) throws IOException {
		connect();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(column));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next())
			printCell(result);
		table.close();
		disconnect();
	}

	public void printCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timestamp: " + cell.getTimestamp() + " ");
			System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}

	/* 
	 * 修改表 tableName，行 row(可以用学生姓名 S_Name 表示)，列 column 指定的单元格的数据
	 */
	public void modifyData(String tableName, String row, String family, String column, String value) throws IOException {
		connect();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(row.getBytes());
		put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
		table.put(put);
		table.close();
		disconnect();
	}

	public void deleteRow(String tableName, String row) throws IOException {
		connect();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(row.getBytes());
		table.delete(delete);
		table.close();
		disconnect();
	}
}