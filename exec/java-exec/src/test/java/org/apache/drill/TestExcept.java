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
package org.apache.drill;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

@Category({SqlTest.class, OperatorTest.class})
public class TestExcept extends ClusterTest {

  private static final String EMPTY_DIR_NAME = "empty_directory";

  @BeforeClass
  public static void setupTestFiles() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("json", "except"));
    dirTestWatcher.makeTestTmpSubDir(Paths.get(EMPTY_DIR_NAME));

    dirTestWatcher.copyResourceToRoot(Paths.get("json", "except", "right"));
    dirTestWatcher.copyResourceToRoot(Paths.get("json", "except", "left"));

    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testExcept() throws Exception {
    String query = "select a from cp.`json/except/right.json` except select a from cp.`json/except/left.json`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a")
        .baselineValuesForSingleColumn(7L)
        .go();
  }

  @Test
  public void testExceptWithCount() throws Exception {
    String query = "select a, count(b) from cp.`json/except/right.json` group by a except select a, count(b) from cp.`json/except/left.json` group by 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a", "EXPR$1")
        .baselineValues(1L, 1L)
        .baselineValues(2L, 3L)
        .baselineValues(4L, 4L)
        .baselineValues(6L, 3L)
        .baselineValues(7L, 1L)
        .baselineValues(8L, 2L)
        .go();
  }

  @Test
  public void testExcept2Fields() throws Exception {
    String query = "select a, count(b) as cnt from cp.`json/except/right.json` group by a except select a, count(b) as cnt from cp.`json/except/left.json` group by a";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a", "cnt")
        .baselineValues(1L, 1L)
        .baselineValues(2L, 3L)
        .baselineValues(4L, 4L)
        .baselineValues(6L, 3L)
        .baselineValues(7L, 1L)
        .baselineValues(8L, 2L)
        .go();
  }

  @Test
  public void testExcept2Fields2() throws Exception {
    String query = "select b, a from cp.`json/except/right.json` except select b, a from cp.`json/except/left.json`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a")
        .baselineValues("e", 4L)
        .baselineValues("f", 4L)
        .baselineValues("g", 2L)
        .baselineValues("h", 2L)
        .baselineValues("i", 6L)
        .baselineValues("j", 6L)
        .baselineValues("k", 6L)
        .baselineValues("l", 7L)
        .baselineValues("m", 8L)
        .baselineValues("n", 8L)
        .go();
  }

  @Test
  public void testExceptAll2Fields2() throws Exception {
    String query = "select b, a from cp.`json/except/right.json` except all select b, a from cp.`json/except/left.json`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a")
        .baselineValues("e", 4L)
        .baselineValues("f", 4L)
        .baselineValues("f", 4L)
        .baselineValues("g", 2L)
        .baselineValues("h", 2L)
        .baselineValues("i", 6L)
        .baselineValues("j", 6L)
        .baselineValues("k", 6L)
        .baselineValues("l", 7L)
        .baselineValues("m", 8L)
        .baselineValues("n", 8L)
        .go();
  }

  @Test
  public void testExceptAll() throws Exception {
    String query = "(select a from cp.`json/except/right.json`) except all (select a from cp.`json/except/left.json`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a")
        .baselineValuesForSingleColumn(2L, 4L, 4L, 4L, 6L, 6L, 7L, 8L)
        .go();
  }

  @Test
  public void testAggregate() throws Exception {
    String query = "select a from cp.`json/except/right.json` group by a";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1L)
        .baselineValues(2L)
        .baselineValues(3L)
        .baselineValues(4L)
        .baselineValues(6L)
        .baselineValues(7L)
        .baselineValues(8L)
        .go();
  }

  @Test
  public void testExcept11() throws Exception {
    String query = "select b, a, c from dfs.`json/except/left` except select b, a, c from dfs.`json/except/right`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a", "c")
        .baselineValues("a", 1L, 1.01D)
        .baselineValues("e", 5L, 3.93D)
        .baselineValues("f", 6L, 4.84D)
        .baselineValues("h", 8L, 6.58D)
        .baselineValues("i", 9L, 6.79D)
        .baselineValues("j", 10L, 3.87D)
        .baselineValues("k", 11L, 4.0D)
        .baselineValues("l", 12L, 3.08D)
        .baselineValues("m", 13L, 0.06D)
        .baselineValues("n", 14L, 8.1D)
        .baselineValues("p", 16L, 2.65D)
        .baselineValues("b2", 2L, 1.52D)
        .baselineValues("c", 3L, -2.19D)
        .baselineValues("d", 4L, 2.79D)
        .baselineValues("e2", 5L, 3.93D)
        .baselineValues("h", 8L, 7.77D)
        .baselineValues("i2", 9L, 6.79D)
        .baselineValues("j", 10L, 0.09D)
        .baselineValues("o", 15L, 2.37D)
        .baselineValues("p2", 16L, 2.65D)
        .baselineValues(null, null, null)
        .baselineValues("q", 17L, 0.86D)
        .baselineValues("r", 18L, 4.43D)
        .baselineValues("s", 19L, 9.06D)
        .baselineValues("t", 20L, 3.98D)
        .baselineValues("u", 21L, 5.74D)
        .baselineValues("v", 22L, 8.12D)
        .baselineValues("w", 23L, -2.99D)
        .baselineValues("x", 24L, 6.91D)
        .baselineValues("y", 25L, 99.22D)
        .baselineValues("y", null, 99.22D)
        .baselineValues("z", 26L, -7.54D)
        .baselineValues("q3", 17L, 0.86D)
        .baselineValues("s", 19L, 7.32D)
        .baselineValues("t", 20L, 9.98D)
        .baselineValues("w3", 23L, -2.99D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("z", 26L, -0.15D)
        .go();
  }

  @Test
  public void testExceptAll11() throws Exception {
    String query = "select b, a, c from dfs.`json/except/left` except all select b, a, c from dfs.`json/except/right`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a", "c")
        .baselineValues("a", 1L, 1.01D)
        .baselineValues("e", 5L, 3.93D)
        .baselineValues("f", 6L, 4.84D)
        .baselineValues("h", 8L, 6.58D)
        .baselineValues("i", 9L, 6.79D)
        .baselineValues("j", 10L, 3.87D)
        .baselineValues("k", 11L, 4.0D)
        .baselineValues("l", 12L, 3.08D)
        .baselineValues("l", 12L, 3.08D)
        .baselineValues("m", 13L, 0.06D)
        .baselineValues("n", 14L, 8.1D)
        .baselineValues("o", 15L, 0.29D) // todo: was commented
        .baselineValues("p", 16L, 2.65D)
        .baselineValues("b2", 2L, 1.52D)
        .baselineValues("c", 3L, -2.19D)
        .baselineValues("d", 4L, 2.79D)
        .baselineValues("e2", 5L, 3.93D)
        .baselineValues("h", 8L, 7.77D)
        .baselineValues("i2", 9L, 6.79D)
        .baselineValues("j", 10L, 0.09D)
        .baselineValues("o", 15L, 2.37D)
        .baselineValues("p2", 16L, 2.65D)
        .baselineValues(null, null, null)
        .baselineValues("q", 17L, 0.86D)
        .baselineValues("r", 18L, 4.43D)
        .baselineValues("s", 19L, 9.06D)
        .baselineValues("t", 20L, 3.98D)
        .baselineValues("u", 21L, 5.74D)
        .baselineValues("v", 22L, 8.12D)
        .baselineValues("w", 23L, -2.99D)
        .baselineValues("x", 24L, 6.91D)
        .baselineValues("y", 25L, 99.22D)
        .baselineValues("y", null, 99.22D)
        .baselineValues("z", 26L, -7.54D)
        .baselineValues("q3", 17L, 0.86D)
        .baselineValues("s", 19L, 7.32D)
        .baselineValues("t", 20L, 9.98D)
        .baselineValues("w3", 23L, -2.99D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("b", 2L, 1.52D) // todo: was commented
        .baselineValues("c", 3L, 2.17D)
        .baselineValues("f", 6L, 4.84D)
        .baselineValues("d", 4L, 3.76D)
        .baselineValues("e", 5L, 3.93D)
        .baselineValues("f", 6L, 4.84D)
        .baselineValues("g", 7L, 5.35D)
        .baselineValues("h", 8L, 6.58D)
        .baselineValues("i", 9L, 6.79D)
        .baselineValues("j", 10L, 3.87D)
        .baselineValues("k", 11L, 4.0D)
        .baselineValues("l", 12L, 3.08D) // todo: was commented
        .baselineValues("m", 13L, 0.06D)
        .baselineValues("n", 14L, 8.1D)
        .baselineValues("t", 20L, 9.98D)
        .baselineValues("w3", 23L, -2.99D)
        .baselineValues("z", 26L, -0.15D)
        .baselineValues("b", 2L, 1.52D)
        .baselineValues("m", 13L, 0.06D)
        .baselineValues("p", 16L, 2.65D)
        .baselineValues("d", 4L, 3.76D)
        .baselineValues("e", 5L, 3.93D)
        .baselineValues("f", 6L, 4.84D)
        .baselineValues("g", 7L, 5.35D)
        .baselineValues("h", 8L, 6.58D)
        .baselineValues("a", 1L, 1.01D)
        .go();
  }
}
