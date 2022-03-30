# HBase 的具体命令参数
1. list: -list
2. print: -print tableName
3. insert: -insert tableName rowKey columnFamily column value
4. insertColumnFamily: -insertColumnFamily tableName columnFamily
5. deleteCol: -deleteCol tableName rowKey columnFamily column
6. deleteColumnFamily: -deleteColumnFamily tableName columnFamily
7. clear: -clear tableName
8. count: -count tableName
9. createTable: -create tableName fields
> fields: field1 field2 field3 ...
10. addRecord: -add tableName rowKey fields values
> fields: field1,field2,field3... \
  values: value1,value2,value3...
11. scanColumn: -scan tableName column
12. modifyData: -modify tableName rowKey column value
13. deleteRow: -deleteRow tableName rowKey