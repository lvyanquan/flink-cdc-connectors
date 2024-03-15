---
title: "Route"
weight: 6
type: docs
aliases:
  - /core-concept/route/
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
Route specifies the target table ID of each event. The most typical scenario is the merge of sub-databases and sub-tables, routing multiple upstream source tables to the same sink table.

To describe a route, the follows are required:   
source-table: Source table id, supports regular expressions     
sink-table: Sink table id, supports regular expressions    
description: Routing rule description(optional, default value provided)    

For example, if synchronize the table `web_order` in the database `mydb` to a Doris table `ods_web_order`, we can use this yaml file to define this route：

```yaml
route:
    source-table: mydb.web_order
    sink-table: mydb.ods_web_order
    description: sync table to one destination table with given prefix ods_
```

What's more, if you want to synchronize the sharding tables in the database `mydb` to a Doris table `ods_web_order`, we can use this yaml file to define this route：
```yaml
route:
    source-table: mydb\.*
    sink-table: mydb.ods_web_order
    description: sync sharding tables to one destination table
```