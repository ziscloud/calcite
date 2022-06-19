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
public class SqlPartitionOptions extends SqlCall {

  protected static final SqlSpecialOperator PARTITION_BY =
      new SqlSpecialOperator("PARTITION BY", SqlKind.PARTITION);

  private final SqlNode type;
  private final @Nullable SqlNode e;
  private final @Nullable SqlNode alg;
  private final @Nullable SqlNode num;
  private final @Nullable SqlNodeList columnList;

  private final @Nullable SqlNodeList partitions;

  /**
   * Creates a SqlKeyConstraint.
   */
  SqlPartitionOptions(PartitionType type, @Nullable SqlNode e, @Nullable SqlNode alg,
      SqlNodeList columnList, @Nullable SqlNode num, @Nullable SqlNodeList partitions,
      SqlParserPos pos) {
    super(pos);
    this.e = e;
    this.columnList = columnList;
    this.type = new SqlIdentifier(type.name(), pos);
    this.alg = alg;
    this.num = num;
    this.partitions = partitions;
  }

  @Override
  public SqlOperator getOperator() {
    return PARTITION_BY;
  }

  @SuppressWarnings("nullness")
  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(type, e, columnList, alg, num, partitions);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("PARTITION BY");
    type.unparse(writer, 0, 0);

    if (alg != null) {
      writer.keyword("ALGORITHM = ");
      alg.unparse(writer, 0, 0);
    }

    if (e != null) {
      e.unparse(writer, 0, 0);
    }

    if (columnList != null) {
      columnList.unparse(writer, 0, 0);
    }

    if (num != null) {
      writer.keyword("PARTITIONS");
      num.unparse(writer, 0, 0);
    }

    if (partitions != null) {
      partitions.unparse(writer, 0, 0);
    }
  }
}
