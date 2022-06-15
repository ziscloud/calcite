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
public class SqlIndex extends SqlCall {

  protected static final SqlSpecialOperator INDEX =
      new SqlSpecialOperator("INDEX", SqlKind.INDEX);

  private final @Nullable SqlIdentifier name;
  private final SqlNodeList columnList;

  private final @Nullable SqlNode comment;

  /** Creates a SqlKeyConstraint. */
  SqlIndex(SqlParserPos pos, @Nullable SqlIdentifier name,
           SqlNodeList columnList, @Nullable SqlNode comment) {
    super(pos);
    this.name = name;
    this.columnList = columnList;
    this.comment = comment;
  }

  @Override
  public SqlOperator getOperator() {
    return INDEX;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (name != null) {
      name.unparse(writer, 0, 0);
    }
    writer.keyword(getOperator().getName());
    if (comment!=null) {
      writer.keyword("CONSTRAINT");
      comment.unparse(writer, 0, 0);
    }
    columnList.unparse(writer, 1, 1);
  }
}
