<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# CarbonData Secondary Index

* [Quick Example](#quick-example)
* [Secondary Index Table](#Secondary-Index-Introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Compaction](#compacting-SI-table)
* [DDLs on Secondary Index](#DDLs-on-Secondary-Index)

## Quick example

Start spark-sql in terminal and run the following queries,
```
CREATE TABLE maintable(a int, b string, c string) stored as carbondata;
insert into maintable select 1, 'ab', 'cd';
CREATE index inex1 on table maintable(c) AS 'carbondata';
SELECT a from maintable where c = 'cd';
// NOTE: run explain query and check if query hits the SI table from the plan
EXPLAIN SELECT a from maintable where c = 'cd';
```

## Secondary Index Introduction
  Sencondary index tables are created as a indexes and managed as child tables internally by
  Carbondata. Users can create secondary index based on the column position in main table(Recommended
  for right columns) and the queries should have filter on that column to improve the filter query
  performance.
  
  SI tables will always be loaded non-lazy way. Once SI table is created, Carbondata's 
  CarbonOptimizer with the help of `CarbonSITransformationRule`, transforms the query plan to hit the
  SI table based on the filter condition or set of filter conditions present in the query.
  So first level of pruning will be done on SI table as it stores blocklets and main table/parent
  table pruning will be based on the SI output, which helps in giving the faster query results with
  better pruning.

  Secondary Index table can be create with below syntax

   ```
   CREATE INDEX [IF NOT EXISTS] index_name
   ON TABLE maintable(index_column)
   AS
   'carbondata'
   [TBLPROPERTIES('table_blocksize'='1')]
   ```
  For instance, main table called **sales** which is defined as

  ```
  CREATE TABLE sales (
    order_time timestamp,
    user_id string,
    sex string,
    country string,
    quantity int,
    price bigint)
  STORED AS carbondata
  ```

  User can create SI table using the Create Index DDL

  ```
  CREATE INDEX index_sales
  ON TABLE sales(user_id)
  AS
  'carbondata'
  TBLPROPERTIES('table_blocksize'='1')
  ```
 
 
#### How SI tables are selected

When a user executes a filter query, during query planning phase, CarbonData with help of
`CarbonSITransformationRule`, checks if there are any index tables present on the filter column of
query. If there are any, then filter query plan will be transformed such a way that, execution will
first hit the corresponding SI table and give input to main table for further pruning.


For the main table **sales** and SI table  **index_sales** created above, following queries
```
SELECT country, sex from sales where user_id = 'xxx'

SELECT country, sex from sales where user_id = 'xxx' and country = 'INDIA'
```

will be transformed by CarbonData's `CarbonSITransformationRule` to query against SI table
**index_sales** first which will be input to the main table **sales**


## Loading data

### Loading data to Secondary Index table(s).

*case1:* When SI table is created and the main table does not have any data. In this case every
consecutive load will load to SI table once main table data load is finished.

*case2:* When SI table is created and main table already contains some data, then SI creation will
also load to SI table with same number of segments as main table. There after, consecutive load to
main table will load to SI table also.

 **NOTE**:
 * In case of data load failure to SI table, then we make the SI table disable by setting a hive serde
 property. The subsequent main table load will load the old failed loads along with current load and
 makes the SI table enable and available for query.

## Querying data
Direct query can be made on SI tables to see the data present in position reference columns.
When a filter query is fired, if the filter column is a secondary index column, then plan is
transformed accordingly to hit SI table first to make better pruning with main table and in turn
helps for faster query results.

User can verify whether a query can leverage SI table or not by executing `EXPLAIN`
command, which will show the transformed logical plan, and thus user can check whether SI table
table is selected.


## Compacting SI table

### Compacting SI table table through Main Table compaction
Running Compaction command (`ALTER TABLE COMPACT`)[COMPACTION TYPE-> MINOR/MAJOR] on main table will
automatically delete all the old segments of SI and creates a new segment with same name as main
table compacted segmet and loads data to it.

### Compacting SI table's individual segment(s) through REBUILD command
Where there are so many small files present in the SI table, then we can use REBUILD command to
compact the files within an SI segment to avoid many small files.

  ```
  REBUILD INDEX sales_index
  ```
This command merges data files in  each segment of SI table.

  ```
  REBUILD INDEX sales_index WHERE SEGMENT.ID IN(1)
  ```
This command merges data files within specified segment of SI table.

## How to skip Secondary Index?
When Secondary indexes are created on a table(s), always data fetching happens from secondary
indexes created on the main tables for better performance. But sometimes, data fetching from the
secondary index might degrade query performance in case where the data is sparse and most of the
blocklets need to be scanned. So to avoid such secondary indexes, we use NI as a function on filters
with in WHERE clause.

  ```
  SELECT country, sex from sales where NI(user_id = 'xxx')
  ```
The above query ignores column user_id from secondary index and fetch data from main table.

## DDLs on Secondary Index

### Show index Command
This command is used to get information about all the secondary indexes on a table.

Syntax
  ```
  SHOW INDEXES  on [db_name.]table_name
  ```

### Drop index Command
This command is used to drop an existing secondary index on a table

Syntax
  ```
  DROP INDEX [IF EXISTS] index_name on [db_name.]table_name
  ```

### Register index Command
This command registers the secondary index with the main table in case of compatibility scenarios 
where we have old stores.

Syntax
  ```
  REGISTER INDEX TABLE index_name ON [db_name.]table_name
  ```