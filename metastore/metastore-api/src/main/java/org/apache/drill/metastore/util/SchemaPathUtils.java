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
package org.apache.drill.metastore.util;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.PrimitiveColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaPathUtils {

  private SchemaPathUtils() {
  }

  /**
   * Returns {@link ColumnMetadata} instance obtained from specified {@code TupleMetadata schema} which corresponds to
   * the specified column schema path.
   *
   * @param schemaPath schema path of the column which should be obtained
   * @param schema     tuple schema where column should be searched
   * @return {@link ColumnMetadata} instance which corresponds to the specified column schema path
   */
  public static ColumnMetadata getColumnMetadata(SchemaPath schemaPath, TupleMetadata schema) {
    PathSegment.NameSegment colPath = schemaPath.getUnIndexed().getRootSegment();
    ColumnMetadata colMetadata = schema.metadata(colPath.getPath());
    while (!colPath.isLastPath() && colMetadata != null) {
      if (colMetadata.isDict()) {
        // get dict's value field metadata
        colMetadata = colMetadata.tupleSchema().metadata(1);
        break;
      }
      if (!colMetadata.isMap()) {
        colMetadata = null;
        break;
      }
      colPath = (PathSegment.NameSegment) colPath.getChild();
      colMetadata = colMetadata.tupleSchema().metadata(colPath.getPath());
    }
    return colMetadata;
  }

  /**
   * Checks if field indetified by the schema path is child in either {@code DICT} or {@code REPEATED MAP}.
   * For such fields, nested in {@code DICT} or {@code REPEATED MAP},
   * filters can't be removed based on Parquet statistics.
   * @param schemaPath schema path used in filter
   * @param schema schema containing all the fields in the file
   * @return {@literal true} if field is nested inside {@code DICT} (is {@code `key`} or {@code `value`})
   *         or inside {@code REPEATED MAP} field, {@literal false} otherwise.
   */
  public static boolean isFieldNestedInDictOrRepeatedMap(SchemaPath schemaPath, TupleMetadata schema) {
    PathSegment.NameSegment colPath = schemaPath.getUnIndexed().getRootSegment();
    ColumnMetadata colMetadata = schema.metadata(colPath.getPath());
    while (!colPath.isLastPath() && colMetadata != null) {
      if (colMetadata.isDict() || (colMetadata.isMap() && Types.isRepeated(colMetadata.majorType()))) {
        return true;
      } else if (!colMetadata.isMap()) {
        break;
      }
      colPath = (PathSegment.NameSegment) colPath.getChild();
      colMetadata = colMetadata.tupleSchema().metadata(colPath.getPath());
    }
    return false;
  }

  /**
   * Adds column with specified schema path and type into specified {@code TupleMetadata schema}.
   * For the case when specified {@link SchemaPath} has children, corresponding maps will be created
   * in the {@code TupleMetadata schema} and the last child of the map will have specified type.
   *
   * @param schema     tuple schema where column should be added
   * @param schemaPath schema path of the column which should be added
   * @param type       type of the column which should be added
   * @param types      list of column's parent types
   */
  public static void addColumnMetadata(TupleMetadata schema, SchemaPath schemaPath, TypeProtos.MajorType type, Map<SchemaPath, TypeProtos.MajorType> types) {
    PathSegment.NameSegment colPath = schemaPath.getUnIndexed().getRootSegment();
    List<String> names = new ArrayList<>(types.size());
    ColumnMetadata colMetadata;
    while (!colPath.isLastPath()) {
      names.add(colPath.getPath());
      colMetadata = schema.metadata(colPath.getPath());
      TypeProtos.MajorType pathType = types.get(SchemaPath.getCompoundPath(names.toArray(new String[0])));

      boolean isDict = pathType != null && pathType.getMinorType() == TypeProtos.MinorType.DICT;
      boolean isList = pathType != null && pathType.getMinorType() == TypeProtos.MinorType.LIST;
      String name = colPath.getPath();

      if (isList) {
        List<String> nextNames = new ArrayList<>(names);
        PathSegment.NameSegment bagSegment = colPath.getChild().getNameSegment();
        PathSegment.NameSegment elementSegment = bagSegment.getChild().getNameSegment();
        nextNames.add(bagSegment.getPath());
        nextNames.add(elementSegment.getPath());

        pathType = types.get(SchemaPath.getCompoundPath(nextNames.toArray(new String[0])));

        if (pathType == null && colPath.getChild().getChild().isLastPath()) {
          // The list is actually a repeated primitive:
          // will be handled after the while statement
          break;
        }

        colPath = elementSegment;

        names.add(bagSegment.getPath());
        names.add(elementSegment.getPath());

        isDict = pathType != null && pathType.getMinorType() == TypeProtos.MinorType.DICT;
      }

      if (colMetadata == null) {
        if (isDict) {
          colMetadata = isList ? MetadataUtils.newDictArray(name, null) : MetadataUtils.newDict(name, null);
        } else {
          colMetadata = isList ? MetadataUtils.newMapArray(name, null) : MetadataUtils.newMap(name, null);
        }
        schema.addColumn(colMetadata);
      }

      if (isDict) {
        // Parquet's MAP (which corresponds to DICT in Drill) has
        // an inner group which we want to skip here
        colPath = (PathSegment.NameSegment) colPath.getChild();
        names.add(colPath.getPath());
      }

      if (!colMetadata.isMap() && !colMetadata.isDict()) {
        throw new DrillRuntimeException(String.format("Expected map or dict, but was %s", colMetadata.majorType()));
      }

      schema = colMetadata.tupleSchema();
      colPath = (PathSegment.NameSegment) colPath.getChild();
    }

    colMetadata = schema.metadata(colPath.getPath());
    if (colMetadata == null) {
      schema.addColumn(new PrimitiveColumnMetadata(MaterializedField.create(colPath.getPath(), type)));
    } else if (!colMetadata.majorType().equals(type)) {
      throw new DrillRuntimeException(String.format("Types mismatch: existing type: %s, new type: %s", colMetadata.majorType(), type));
    }
  }
}
