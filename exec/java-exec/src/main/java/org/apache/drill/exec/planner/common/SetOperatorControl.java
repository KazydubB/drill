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
package org.apache.drill.exec.planner.common;

public class SetOperatorControl {

//  public final static int DEFAULT = 0;
//  public final static int INTERSECT_DISTINCT = 0x01;//0001
//  public final static int INTERSECT_ALL = 0x03; //0011
//  public final static int INTERSECT_MASK = 0x03;
//  public final static int EXCEPT_DISTINCT = 0x04;
//  public final static int EXCEPT_ALL = 0x05;
//  private final int joinControl;

  public final static int EXCEPT = 0;
  public final static int INTERSECT = 1;

  private final int type;
  private final boolean all;

  public SetOperatorControl(int type, boolean all) {
    this.type = type;
    this.all = all;
  }

  public boolean isIntersect() {
    return isIntersectAll() || isIntersectDistinct();
  }

  public boolean isIntersectDistinct() {
    return type == INTERSECT && !all;
  }

  public boolean isIntersectAll() {
    return type == INTERSECT && all;
  }

  public boolean isExcept() {
    return isExceptAll() || isExceptDistinct();
  }

  public boolean isExceptDistinct() {
    return type == EXCEPT && !all;
  }

  public boolean isExceptAll() {
    return type == EXCEPT && all;
  }

  public boolean isAll() {
    return all;
  }
}
