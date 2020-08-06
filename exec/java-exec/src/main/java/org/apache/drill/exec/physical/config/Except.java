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
package org.apache.drill.exec.physical.config;

import java.util.List;

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.AbstractMultiple;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName("except")
public class Except extends AbstractMultiple {

  static final Logger logger = LoggerFactory.getLogger(Except.class);

  private final List<String> leftFields;
  private final List<String> rightFields;

  private final List<NamedExpression> leftExpressions;
  private final List<NamedExpression> rightExpressions;

  private final boolean all;

  @JsonCreator
  public Except(@JsonProperty("children") List<PhysicalOperator> children,
                @JsonProperty("leftFields") List<String> leftFields,
                @JsonProperty("rightFields") List<String> rightFields,
                @JsonProperty("leftExpressions") List<NamedExpression> leftExpressions,
                @JsonProperty("rightExpressions") List<NamedExpression> rightExpressions,
                @JsonProperty("all") boolean all) {
    super(children);
    this.leftFields = leftFields;
    this.rightFields = rightFields;
    this.leftExpressions = leftExpressions;
    this.rightExpressions = rightExpressions;
    this.all = all;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitExcept(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new Except(children, leftFields, rightFields, leftExpressions, rightExpressions, all);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.EXCEPT_VALUE;
  }

  public List<String> getLeftFields() {
    return leftFields;
  }

  public List<String> getRightFields() {
    return rightFields;
  }

  public List<NamedExpression> getLeftExpressions() {
    return leftExpressions;
  }

  public List<NamedExpression> getRightExpressions() {
    return rightExpressions;
  }

  public boolean isAll() {
    return all;
  }

  public boolean isDistinct() {
    return !all;
  }
}
