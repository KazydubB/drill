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
package org.apache.drill.exec.physical.impl.except;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.config.Except;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.set.HashSetRecordBatch;
import org.apache.drill.exec.planner.common.SetOperatorControl;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ExceptBatchCreator implements BatchCreator<Except> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, Except config,
        List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 2);
    SetOperatorControl control = config.createOperatorControl();
//    RecordBatch left;
//    RecordBatch right;
//    if (control.isDistinct() /*|| control.isAll()*/) {
//      left = children.get(1);
//      right = children.get(0);
//    } else {
//      left = children.get(0);
//      right = children.get(1);
//    }
    return new HashSetRecordBatch(config, context, children.get(0), children.get(1));
  }
}
