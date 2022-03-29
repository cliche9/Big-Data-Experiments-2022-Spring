# HBase 的具体命令参数
1. list: -list
2. print: -print tableName
3. insert: -insert tableName rowKey columnFamily column value
4. deleteCol: -deleteCol tableName rowKey columnFamily column
5. clear: -clear tableName
6. count: -count tableName
7. createTable: -create tableName fields
> fields: field1 field2 field3 ...
8. addRecord: -add tableName rowKey fiedls values
> fields: field1,field2,field3... \
  values: value1,value2,value3...
9. scanColumn: -scan tableName column
10. modifyData: -modify tableName rowKey column value
11. deleteRow: -deleteRow tableName rowKey