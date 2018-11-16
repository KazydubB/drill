/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.materialize;

import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.server.options.OptionManager;

public class VectorRecordMaterializer implements RecordMaterializer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorRecordMaterializer.class);

  private QueryId queryId;
  private RecordBatch batch;
  private BufferAllocator allocator;
  private OptionManager options;

  public VectorRecordMaterializer(FragmentContext context, OperatorContext oContext, RecordBatch batch) {
    this.queryId = context.getHandle().getQueryId();
    this.batch = batch;
    this.allocator = oContext.getAllocator();
    BatchSchema schema = batch.getSchema();
    assert schema != null : "Schema must be defined.";
    options = context.getSessionOptions();

//    for (MaterializedField f : batch.getSchema()) {
//      logger.debug("New Field: {}", f);
//    }
  }
// todo: provide correct value!
  public QueryWritableBatch convertNext() {
    //batch.getWritableBatch().getDef().getRecordCount()
    WritableBatch w = batch.getWritableBatch().transfer(allocator);
    // todo: maybe pass some info about "affects-row" to header here?
    QueryData.Builder builder = QueryData.newBuilder() //
        .setQueryId(queryId) //
        .setRowCount(batch.getRecordCount()) //
        .setDef(w.getDef());
    if (options != null && !options.getOption(ExecConstants.FETCH_RESULT_SET_VALIDATOR)) {
      String kindString = options.getString("sqlnode.kind");
      SqlKind kind = SqlKind.valueOf(kindString);
      if (SqlKind.DDL.contains(kind)) {
        int count = w.getDef().getUpdateCount();
        builder.setUpdateCount(count == -1 ? 0 : count); // todo: get from def?
      }
    }
    return new QueryWritableBatch(builder.build(), w.getBuffers());
  }
}
