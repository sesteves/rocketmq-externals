/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.hbase;

import java.util.LinkedList;
import java.util.List;

public class Transaction {

    private Config config;

    private List<DataRow> list = new LinkedList<>();

    public Transaction(Config config) {
        this.config = config;
    }

    public boolean addRow() {
        if(list.size() == config.getMaxTransactionRows()) {
            return false;
        }

        DataRow dataRow = new DataRow();
        list.add(dataRow);
        return true;
    }

    public String toJson() {
        // TODO implement

        return null;
    }

}
