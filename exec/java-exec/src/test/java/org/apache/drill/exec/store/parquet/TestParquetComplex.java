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
package org.apache.drill.exec.store.parquet;

import java.math.BigDecimal;

import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.TestBuilder;
import org.junit.Test;

public class TestParquetComplex extends BaseTestQuery {

  private static final String DATAFILE = "cp.`store/parquet/complex/complex.parquet`";

  @Test
  public void sort() throws Exception {
    String query = String.format("select * from %s order by amount", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void topN() throws Exception {
    String query = String.format("select * from %s order by amount limit 5", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void hashJoin() throws Exception{
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void mergeJoin() throws Exception{
    test("alter session set `planner.enable_hashjoin` = false");
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectAllColumns() throws Exception {
    String query = String.format("select amount, `date`, marketing_info, `time`, trans_id, trans_info, user_info from %s", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectMap() throws Exception {
    String query = "select marketing_info from cp.`store/parquet/complex/complex.parquet`";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline5.json")
            .build()
            .run();
  }

  @Test
  public void selectTrueMap() throws Exception {
    String query = "select * from cp.`store/parquet/complex/simple_map.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("order_items")
        .baselineValues(TestBuilder.mapOfObject("Pencils", 1L))
        .go();
  }

  @Test
  public void selectTrueMap2() throws Exception {
    String query = "select order_id, order_items from cp.`store/parquet/complex/map/m_a.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("order_id", "order_items")
        .baselineValues(1L,
              TestBuilder.mapOfObject(
                  101L,
                  TestBuilder.mapOfObject(false, "item_amount", 1L, "item_type", "Pencils"),
                  102L,
                  TestBuilder.mapOfObject(false, "item_amount", 2L, "item_type", "Eraser")
              )
        )
        .baselineValues(1L,
              TestBuilder.mapOfObject(
                  102L,
                  TestBuilder.mapOfObject(false, "item_amount", 3L, "item_type", "Eraser"),
                  103L,
                  TestBuilder.mapOfObject(false, "item_amount", 4L, "item_type", "Coke")
            )
        )
        .go();
  }

  @Test
  public void selectTrueMap3() throws Exception {
//    String query = "select * from cp.`store/parquet/complex/map/map_int_to_int_array.parquet` order by 1 desc";
    String query = "select id, mapcol from cp.`store/parquet/complex/map/map_int_to_int_array.parquet` order by id asc"; // todo: order by is not working
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "mapcol")
        .baselineValues(1,
            TestBuilder.mapOfObject(1, TestBuilder.listOf(1, 2, 3, 4, 5))
        )
        .baselineValues(2,
            TestBuilder.mapOfObject(1, TestBuilder.listOf(1, 2, 3, 4, 5), 2, TestBuilder.listOf(2, 3))
        )
        .baselineValues(3,
            TestBuilder.mapOfObject(3, TestBuilder.listOf(3, 4, 5), 5, TestBuilder.listOf(5, 3))
        )
        .go();
  }

  @Test
  public void selectTrueMap4() throws Exception {
    //String query = "select mapcol[2] from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet`"; // todo: add order by `id`
    String query = "select * from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet`"; // todo: add order by `id`
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "mapcol")
        .baselineValues(2, TestBuilder.mapOfObject(
              3, TestBuilder.mapOfObject("a", 1, "b", 2),
              4, TestBuilder.mapOfObject("c", 3),
              5, TestBuilder.mapOfObject("d", 4, "e", 5)
            )
        )
        .baselineValues(1, TestBuilder.mapOfObject(
              1, TestBuilder.mapOfObject("a", 1, "b", 2)
            )
        )
        .baselineValues(2, TestBuilder.mapOfObject(
              2, TestBuilder.mapOfObject("a", 1, "b", 2),
              3, TestBuilder.mapOfObject("c", 3)
            )
        )
        .go();
  }

  @Test
  public void selectTrueMap5() throws Exception {
    // 1, {1 : [1, 2, 3, 4, 5]}
    // 2, {1 : [1, 2, 3, 4, 5], 2 : [2, 3]}
    // 3, {3 : [3, 4, 5], 5 : [5, 3]}
//    String query = "select * from cp.`store/parquet/complex/map/map_int_to_int_array.parquet` order by 1 desc";
    // todo: make sure the result is OK!
    String query = "select id, flatten(mapcol) as flat from cp.`store/parquet/complex/map/map_int_to_int_array.parquet` order by id";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "flat")
        .baselineValues(1,
            TestBuilder.mapOfObject(false, "key", 1, "value", TestBuilder.listOf(1, 2, 3, 4, 5))
        )
        .baselineValues(2,
            TestBuilder.mapOfObject(false, "key", 1, "value", TestBuilder.listOf(1, 2, 3, 4, 5))
        )
        .baselineValues(2,
            TestBuilder.mapOfObject(false, "key", 2, "value", TestBuilder.listOf(2, 3))
        )
        .baselineValues(3,
            TestBuilder.mapOfObject(false, "key", 3, "value", TestBuilder.listOf(3, 4, 5))
        )
        .baselineValues(3,
            TestBuilder.mapOfObject(false, "key", 5, "value", TestBuilder.listOf(5, 3))
        )
        .go();
  }

  @Test
  public void selectTrueMapFlattenListValue() throws Exception {
    // 1, {1 : [1, 2, 3, 4, 5]}
    // 2, {1 : [1, 2, 3, 4, 5], 2 : [2, 3]}
    // 3, {3 : [3, 4, 5], 5 : [5, 3]}
//    String query = "select * from cp.`store/parquet/complex/map/map_int_to_int_array.parquet` order by 1 desc";
    // todo: As empty lists are not accounted for, make sure the result is OK!
    String query = "select id, flatten(mapcol[1]) as flat from cp.`store/parquet/complex/map/map_int_to_int_array.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "flat")
        .baselineValues(1, 1)
        .baselineValues(1, 2)
        .baselineValues(1, 3)
        .baselineValues(1, 4)
        .baselineValues(1, 5)
        .baselineValues(2, 1)
        .baselineValues(2, 2)
        .baselineValues(2, 3)
        .baselineValues(2, 4)
        .baselineValues(2, 5)
        .go();
  }

  @Test
  public void selectTrueMap6() throws Exception {
//    String query = "select id, mapcol[2] as mapcol from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet`"; // todo: add order by `id`
    String query = "select id, mapcol from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet`"; // todo: add order by `id`
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("id", "mapcol")
        .baselineValues(2, TestBuilder.mapOfObject(
            3, TestBuilder.mapOfObject("a", 1, "b", 2),
            4, TestBuilder.mapOfObject("c", 3),
            5, TestBuilder.mapOfObject("d", 4, "e", 5)
            )
        )
        .baselineValues(1, TestBuilder.mapOfObject(
            1, TestBuilder.mapOfObject("a", 1, "b", 2)
            )
        )
        .baselineValues(2, TestBuilder.mapOfObject(
            2, TestBuilder.mapOfObject("a", 1, "b", 2),
            3, TestBuilder.mapOfObject("c", 3)
            )
        )
        .go();
  }

  @Test
  public void selectTrueMapByKeyPrimitiveValue() throws Exception {
//        4, {"b": null, "c": 8, "d": 9, "e": 10}
//        3, {"b": 6, "c": 7}
//        2, {"a": 3, "b": 4, "c": 5}
//        1, {"a": 1, "b": 2, "c": 3}
    String fileName = "map_where.parquet";
    String query = "select mapcol['a'] val from cp.`store/parquet/complex/map/%s` order by id desc";
    testBuilder()
        .sqlQuery(query, fileName)
        .ordered()
        .baselineColumns("val") // todo: make sure this is actually correct
        .baselineValuesForSingleColumn(null, null, 3, 1)
        .go();
  }

  @Test
  public void selectTrueMapByKeyPrimitiveValue2() throws Exception {
//        4, {"b": null, "c": 8, "d": 9, "e": 10}
//        3, {"b": 6, "c": 7}
//        2, {"a": 3, "b": 4, "c": 5}
//        1, {"a": 1, "b": 2, "c": 3}
    String fileName = "map_where.parquet";
    String query = "select id, mapcol['b'] val from cp.`store/parquet/complex/map/%s` order by id desc"; // todo: add id to select
    testBuilder()
        .sqlQuery(query, fileName)
        .ordered()
        .baselineColumns("id", "val") // todo: make sure this is actually correct
        .baselineValues(4, null)
        .baselineValues(3, 6)
        .baselineValues(2, 4)
        .baselineValues(1, 2)
        .go();
  }

  @Test
  public void selectTrueMapByKeyComplexValue() throws Exception {
//        2, {3: {"a", 1, "b", 2}, 4: {"c", 3}, 5: {"d", 4, "e", 5}}
//        1, {1: {"a", 1, "b", 2}}
//        2, {2: {"a", 1, "b", 2}, 3: {"c", 3}}
    String query = "select mapcol[2] val from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet` order by id"; // todo: order by id doesn't work
//    String query = "select mapcol[2] val from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet`"; // todo: add order by `id`
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("val")
        .baselineValues(TestBuilder.mapOfObject())
        .baselineValues(TestBuilder.mapOfObject())
        .baselineValues(TestBuilder.mapOfObject("a", 1, "b", 2))
        .go();
  }

  @Test
  public void selectTrueMapByKeyComplexValue2() throws Exception {
    // todo: this seem to work correctly just need to look at Hive's order for rows with id = 2 for asc and desc
    //        1, {1: {"a", 1, "b", 2}}
    //        2, {3: {"a", 1, "b", 2}, 4: {"c", 3}, 5: {"d", 4, "e", 5}}
    //        2, {2: {"a", 1, "b", 2}, 3: {"c", 3}}
    // todo: uncommnet!
//    String query = "select id, mapcol[3] from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet` order by id asc"; // todo: order by `id` desc doesn't work
    String query = "select id, mapcol[3], mapcol[4]['c'] from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet` order by id asc";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("id", "EXPR$1", "EXPR$2")
        .baselineValues(
            1, TestBuilder.mapOfObject(), null
        )
        .baselineValues(
            2, TestBuilder.mapOfObject("a", 1, "b", 2), 3
        )
        .baselineValues(
            2, TestBuilder.mapOfObject("c", 3), null
        )
        .go();
  }

  @Test
  public void selectTrueMapByKeyComplexValue3() throws Exception {
    // todo: this seem to work correctly just need to look at Hive's order for rows with id = 2 for asc and desc
    //        1, {1: {"a", 1, "b", 2}}
    //        2, {3: {"a", 1, "b", 2}, 4: {"c", 3}, 5: {"d", 4, "e", 5}}
    //        2, {2: {"a", 1, "b", 2}, 3: {"c", 3}}
    String query = "select id, mapcol[3]['b'] val from cp.`store/parquet/complex/map/map_int_to_map_string_to_int.parquet` order by id asc";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("id", "val")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .baselineValues(2, null)
        .go();
  }

  @Test
  public void testOrderById() throws Exception {
    String fileName = "map_where.parquet";
    // String fileName = "map_duplicate.parquet";
    // String query = "select order_items from cp.`store/parquet/complex/simple_map.parquet`";
    // String query = "select * from cp.`store/parquet/complex/map/parquet_map_table_1.parquet`";
    // String query = "select id, mapcol from cp.`store/parquet/complex/map/parquet_map_table_1.parquet` where mapcol['Eraser'] > 5";
    // String query = "select order_id, order_items[101] from cp.`store/parquet/complex/map/%s`";
    String query = "select id, mapcol from cp.`store/parquet/complex/map/%s` order by id desc"; // todo: !!
    // String query = "select order_id, order_items from cp.`store/parquet/complex/map/%s` where order_items[cast(101 as bigint)] is not null";
    // String query = "select order_id, order_items from cp.`store/parquet/complex/map/%s`";
    // String query = "select id, mapcol from cp.`store/parquet/complex/map/%s` where mapcol['b'] > 3";
    // String query = "select id, mapcol from cp.`store/parquet/complex/map/%s` where mapcol['b'] = 2";
    // String query = "select a from cp.`store/parquet/complex/map/a.json` where a['b'] > 1";
    // String query = "select id, mapcol['b'] from cp.`store/parquet/complex/map/%s`";
    // String query = "select order_items[101] from cp.`store/parquet/complex/map/%s`";
    // String query = "select flatten(order_items) from cp.`store/parquet/complex/map/%s`";
    // String query = "select id, mapcol from cp.`store/parquet/complex/map/%s`";
    //String query = "select mapcol from cp.`store/parquet/complex/map/`";
    // String query = "SELECT * FROM hive.parquet_map_table";
    /*testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 0, 2)
        .go();*/
//    query = "select FLOAT4_col from cp.`parquet/alltypes_repeated.json` order by FLOAT4_col desc";
    // runAndPrint(String.format(query, fileName));
    testBuilder()
        .sqlQuery(query, fileName)
        .ordered()
        .baselineColumns("id", "mapcol") // todo: make sure this is actually correct
        .baselineValues(4, TestBuilder.mapOfObject("b", null, "c", 8, "d", 9, "e", 10))
        .baselineValues(3, TestBuilder.mapOfObject("b", 6, "c", 7))
        .baselineValues(2, TestBuilder.mapOfObject("a", 3, "b", 4, "c", 5))
        .baselineValues(1, TestBuilder.mapOfObject("a", 1, "b", 2, "c", 3))
        .go();
  }

  @Test
  public void testOrderByIdLimit() throws Exception {
    String fileName = "map_where.parquet";
    String query = "select id, mapcol from cp.`store/parquet/complex/map/%s` order by id desc limit 2 offset 2"; // todo: !!
    testBuilder()
        .sqlQuery(query, fileName)
        .ordered()
        .baselineColumns("id", "mapcol") // todo: make sure this is actually correct
//        .baselineValues(4, TestBuilder.mapOfObject("b", null, "c", 8, "d", 9, "e", 10))
//        .baselineValues(3, TestBuilder.mapOfObject("b", 6, "c", 7))
        .baselineValues(2, TestBuilder.mapOfObject("a", 3, "b", 4, "c", 5))
        .baselineValues(1, TestBuilder.mapOfObject("a", 1, "b", 2, "c", 3))
        .go();
  }

  @Test
  public void trueMapArray() throws Exception {
//    String query = "select typeOf(mapcol) as mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
//    String query = "select map_array[0] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    String query = "select id, mapcol, map_array from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
//    String query = "select id, mapcol, map_array[0] as element from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
//      .baselineColumns("map_array")
//      .baselineValues(TestBuilder.listOf(TestBuilder.mapOf()))
//        .baselineColumns("val")
         .baselineColumns("id", "mapcol", "map_array")
//         .baselineColumns("id", "mapcol", "element")
        .baselineValues(
            3,
            TestBuilder.mapOfObject(7L, 1, 102L, 2, 524L, 3, 901920L, 4),
            TestBuilder.listOf(
                TestBuilder.mapOfObject(8L, 1, 9L, 2, 523L, 4, 31L, 3),
                TestBuilder.mapOfObject(1L, 2, 3L, 1, 5L, 3)
            )
        )
        .baselineValues(
            1,
            TestBuilder.mapOfObject(1L, 1, 2L, 2),
            TestBuilder.listOf(
                TestBuilder.mapOfObject(1L, 1, 2L, 2)
            )
        )
        .baselineValues(
            2,
            TestBuilder.mapOfObject(3L, 1, 1L, 2, 5L, 3),
            TestBuilder.listOf(
                TestBuilder.mapOfObject(3L, 1),
                TestBuilder.mapOfObject(1L, 2)
            )
        )
        .go();
  }

  @Test
  public void trueMapArrayByIndex() throws Exception {
//    String query = "select typeOf(mapcol) as mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
//    String query = "select map_array[0] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
//    String query = "select id, mapcol, map_array from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    String query = "select id, map_array[0] as element, mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet` order by id desc";
    testBuilder()
        .sqlQuery(query)
        .ordered()
//      .baselineColumns("map_array")
//      .baselineValues(TestBuilder.listOf(TestBuilder.mapOf()))
//        .baselineColumns("val")
//         .baselineColumns("id", "mapcol", "map_array")
        .baselineColumns("id", "element", "mapcol")
        .baselineValues(
            3,
            TestBuilder.mapOfObject(8L, 1, 9L, 2, 523L, 4, 31L, 3),
            TestBuilder.mapOfObject(7L, 1, 102L, 2, 524L, 3, 901920L, 4)
        )
        .baselineValues(
            2,
            TestBuilder.mapOfObject(3L, 1),
            TestBuilder.mapOfObject(3L, 1, 1L, 2, 5L, 3)
        )
        .baselineValues(
            1,
            TestBuilder.mapOfObject(1L, 1, 2L, 2),
            TestBuilder.mapOfObject(1L, 1, 2L, 2)
        )
        .go();
  }

  @Test
  public void trueMapArrayGetByLongKey() throws Exception {
//    String query = "select typeOf(mapcol) as mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    String query = "select id, mapcol[1] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
//      .baselineColumns("map_array")
//      .baselineValues(TestBuilder.listOf(TestBuilder.mapOf()))
        .baselineColumns("id", "val")
        // .baselineColumns("id", "mapcol", "map_array")
        .baselineValues(3, null)
        .baselineValues(1, 1)
        .baselineValues(2, 2)
        .go();
  }

  @Test
  public void trueMapArrayGetByLongKey2() throws Exception {
//    String query = "select typeOf(mapcol) as mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    String query = "select mapcol[1] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
//      .baselineColumns("map_array")
//      .baselineValues(TestBuilder.listOf(TestBuilder.mapOf()))
        .baselineColumns("val")
        // .baselineColumns("id", "mapcol", "map_array")
        .baselineValues((Integer) null)
        .baselineValues(1)
        .baselineValues(2)
        .go();
  }

  @Test
  public void trueMapArrayByIndex2() throws Exception {
//    String query = "select typeOf(mapcol) as mapcol from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    String query = "select map_array[1][5] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet` order by map_array[1][5] desc";
//    String query = "select map_array[1] as val from cp.`store/parquet/complex/map/map_and_map_array.parquet`";
    testBuilder()
        .sqlQuery(query)
        .ordered()
//      .baselineColumns("map_array")
//      .baselineValues(TestBuilder.listOf(TestBuilder.mapOf()))
        .baselineColumns("val")
        // .baselineColumns("id", "mapcol", "map_array")
        .baselineValues(
            new Object[] {null}
//            TestBuilder.listOf(
//                TestBuilder.mapOfObject(8L, 1, 9L, 2, 523L, 4, 31L, 3)
//                TestBuilder.mapOfObject(1L, 2, 3L, 1, 5L, 3)
//            )
        )
        .baselineValues(
            new Object[] {null}
//            TestBuilder.listOf(
//                TestBuilder.mapOfObject(1L, 1, 2L, 2)
//            )
        )
        .baselineValues(
            3
//            TestBuilder.listOf(
//                TestBuilder.mapOfObject(3L, 1)
//                TestBuilder.mapOfObject(1L, 2)
//            )
        )
        .go();
  }

  @Test
  public void selectTypeOfTrueMap() throws Exception {
    String query = "select typeof(map_array[0]) as type from cp.`store/parquet/complex/map/map_and_map_array.parquet` limit 1";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("type")
        .baselineValuesForSingleColumn("TRUEMAP<BIGINT,INT>")
        .go();
  }

  @Test
  public void selectMapAndElements() throws Exception {
    String query = "select marketing_info, t.marketing_info.camp_id as camp_id, t.marketing_info.keywords[2] as keyword2 from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline6.json")
            .build()
            .run();
  }

  @Test
  public void selectMultiElements() throws Exception {
    String query = "select t.marketing_info.camp_id as camp_id, t.marketing_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline7.json")
            .build()
            .run();
  }

  @Test
  public void testStar() throws Exception {
    testBuilder()
            .sqlQuery("select * from cp.`store/parquet/complex/complex.parquet`")
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void missingColumnInMap() throws Exception {
    String query = "select t.trans_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline2.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void secondElementInMap() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords as keywords from %s t", DATAFILE);
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline3.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArray() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords[0] as keyword0, t.`marketing_info`.keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArrayCaseInsensitive() throws Exception {
    String query = String.format("select t.`MARKETING_INFO`.keywords[0] as keyword0, t.`Marketing_Info`.Keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  // todo: check generated ProjectorGen for now and before
  @Test //DRILL-3533
  public void notxistsField() throws Exception {
    String query = String.format("select t.`marketing_info`.notexists as notexists1,\n" +
                                        "t.`marketing_info`.camp_id as id,\n" +
                                        "t.`marketing_info.camp_id` as notexists2\n" +
                                  "from %s t", DATAFILE);
    String[] columns = {"notexists1", "id", "notexists2"};
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("store/parquet/complex/baseline8.json")
        .baselineColumns(columns)
        .build()
        .run();
  }

  public void testReadRepeatedDecimals() throws Exception {

    JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
    ints.add(new BigDecimal("999999.999"));
    ints.add(new BigDecimal("-999999.999"));
    ints.add(new BigDecimal("0.000"));

    JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
    longs.add(new BigDecimal("999999999.999999999"));
    longs.add(new BigDecimal("-999999999.999999999"));
    longs.add(new BigDecimal("0.000000000"));

    JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
    fixedLen.add(new BigDecimal("999999999999.999999"));
    fixedLen.add(new BigDecimal("-999999999999.999999"));
    fixedLen.add(new BigDecimal("0.000000"));

    testBuilder()
        .sqlQuery("select * from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`")
        .unOrdered()
        .baselineColumns("decimal_int32", "decimal_int64", "decimal_fixedLen", "decimal_binary")
        .baselineValues(ints, longs, fixedLen, fixedLen)
        .go();
  }
}
