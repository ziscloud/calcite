/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 *
 * <p>And {@code FOREIGN KEY}, when we support it.
 */
public class SqlPartition extends SqlCall {

  protected static final SqlSpecialOperator PARTITION =
      new SqlSpecialOperator("PARTITION", SqlKind.PARTITION);

  private final @Nullable SqlIdentifier name;
  private final @Nullable SqlIdentifier type;
  private final @Nullable SqlNode e;
  private final SqlNodeList valueList;

  private final @Nullable SqlNode max;
  private final @Nullable SqlIdentifier engineName;
  private final @Nullable SqlNode comment;
  private final @Nullable SqlNode dataDir;
  private final @Nullable SqlNode indexDir;
  private final @Nullable SqlNode maxRow;
  private final @Nullable SqlNode minRow;
  private final @Nullable SqlNode tablespace;

  /** Creates a SqlKeyConstraint. */
  SqlPartition(@Nullable SqlIdentifier name, @Nullable SqlIdentifier type, @Nullable SqlNode e,
      SqlNodeList valueList, @Nullable SqlNode max, @Nullable SqlIdentifier engineName,
      @Nullable SqlNode comment, @Nullable SqlNode dataDir, @Nullable SqlNode indexDir,
      @Nullable SqlNode maxRow, @Nullable SqlNode minRow, @Nullable SqlNode tablespace,
      SqlParserPos pos) {
    super(pos);
    this.name = name;
    this.valueList = valueList;
    this.comment = comment;
    this.e = e;
    this.max = max;
    this.engineName = engineName;
    this.dataDir = dataDir;
    this.indexDir = indexDir;
    this.maxRow = maxRow;
    this.minRow = minRow;
    this.tablespace = tablespace;
    this.type = type;
  }

  @Override
  public SqlOperator getOperator() {
    return PARTITION;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, valueList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    if (name != null) {
      name.unparse(writer, 0, 0);
    }
    writer.keyword("VALUES");
    if (type.getSimple().equals("LESS")) {
      writer.keyword("LESS THAN");
      if(e != null) {
        e.unparse(writer, 0, 0);
      }

      unparseValueList(writer);

      if (max != null) {
        writer.keyword("MAXVALUE");
      }
    }
    if (type.getSimple().equals("IN")) {
      writer.keyword("IN");
      unparseValueList(writer);
    }
    if (engineName != null) {
      writer.keyword("ENGINE");
      writer.keyword(engineName.toString());
    }
    if (comment!=null) {
      writer.keyword("COMMENT");
      comment.unparse(writer, 0, 0);
    }
    if (dataDir!=null) {
      writer.keyword("DATA DIRECTORY");
      dataDir.unparse(writer, 0, 0);
    }

    if (indexDir!=null) {
      writer.keyword("INDEX DIRECTORY");
      indexDir.unparse(writer, 0, 0);
    }

    if (maxRow!=null) {
      writer.keyword("MAX_ROWS");
      maxRow.unparse(writer, 0, 0);
    }
    if (minRow!=null) {
      writer.keyword("MIN_ROWS");
      minRow.unparse(writer, 0, 0);
    }
    if (tablespace!=null) {
      writer.keyword("TABLESPACE");
      tablespace.unparse(writer, 0, 0);
    }
  }

  private void unparseValueList(SqlWriter writer) {
    if (valueList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode a : valueList) {
        writer.sep(",");
        if ("MAXVALUE".equals(a.toString())) {
          writer.keyword("MAXVALUE");
        } else {
          a.unparse(writer, 0, 0);
        }
      }
      writer.endList(frame);
    }
  }
}
