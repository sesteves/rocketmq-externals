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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;

import java.util.LinkedList;
import java.util.List;

public class Transaction {

    private static final String ROCKETMQ_TRANSACTION_ROWS_PARAM = "rocketmq.transaction.max.rows";
    private static final int ROCKETMQ_TRANSACTION_ROWS_DEFAULT = 100;

    private final int maxTransactionRows;

    private List<DataRow> list = new LinkedList<>();

    public Transaction(Configuration config) {
        maxTransactionRows = config.getInt(ROCKETMQ_TRANSACTION_ROWS_PARAM, ROCKETMQ_TRANSACTION_ROWS_DEFAULT);
    }

    public boolean addRow(byte[] rowKey, List<Cell> cells) {
        if(list.size() == maxTransactionRows) {
            return false;
        }

        final Cell cell = cells.get(0);
        final CellProtos.CellType type = CellProtos.CellType.valueOf(cell.getTypeByte());
        switch (type) {
            case DELETE:
            case DELETE_COLUMN:
            case DELETE_FAMILY:
//                rowOp = HRow.RowOp.DELETE;
                break;
            case PUT:
//                rowOp = HRow.RowOp.PUT;
                break;
        }


        DataRow dataRow = new DataRow(rowKey, cells);
        list.add(dataRow);
        return true;
    }

    private void toRowColumns(final List<Cell> cells) {

//        cells.stream().map(cell -> {
//            byte[] family = CellUtil.cloneFamily(cell);
//            byte[] qualifier = CellUtil.cloneQualifier(cell);
//            byte[] value = CellUtil.cloneValue(cell);
//            long timestamp = cell.getTimestamp();
//
//            final HRow.HColumn column = new HRow.HColumn(family, qualifier, value, timestamp);
//            return column;
//        }).collect(toList());
//
//        return columns;
    }

    public String toJson() {
        // TODO implement

        return null;
    }

}
