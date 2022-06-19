<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
}
{
    <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.end(this), replace, ifNotExists, id);
    }
}

SqlCreate SqlCreateForeignSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNode type = null;
    SqlNode library = null;
    SqlNodeList optionList = null;
}
{
    <FOREIGN> <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
         <TYPE> type = StringLiteral()
    |
         <LIBRARY> library = StringLiteral()
    )
    [ optionList = Options() ]
    {
        return SqlDdlNodes.createForeignSchema(s.end(this), replace,
            ifNotExists, id, type, library, optionList);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableOptions() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }

        TableOption(list)
        (
            [<COMMA>]
            TableOption(list)
        )*

    {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableOption(List<SqlNode> list) :
{
    final Span s;
    final SqlTableOptionName id;
    final SqlNode value;
}
{
    { s = span(); } id = TableOptionName()
    <EQ>
    (
        value = Literal()
    |
        value = SimpleIdentifier()
    |
        <DEFAULT_> {value = new SqlIdentifier("DEFAULT", s.end(this));}
    |
        <DYNAMIC> {value = new SqlIdentifier("DYNAMIC", s.end(this));}
    |
        <NO> {value = new SqlIdentifier("NO", s.end(this));}
    )
    {
        list.add(new SqlTableOption(id, value, s.end(this)));
    }
}

SqlTableOptionName TableOptionName() :
{
}
{
  <AUTOEXTEND_SIZE> { return SqlTableOptionName.AUTOEXTEND_SIZE;}
| <AUTO_INCREMENT> { return SqlTableOptionName.AUTO_INCREMENT;}
| <AVG_ROW_LENGTH> { return SqlTableOptionName.AVG_ROW_LENGTH;}
| [<DEFAULT_>] <CHARACTER> <SET> { return SqlTableOptionName.CHARACTER_SET;}
| <CHECKSUM> { return SqlTableOptionName.CHECKSUM;}
| [<DEFAULT_>] <COLLATE> { return SqlTableOptionName.COLLATE;}
| <COMMENT> { return SqlTableOptionName.COMMENT;}
| <COMPRESSION> { return SqlTableOptionName.COMPRESSION;}
| <CONNECTION> { return SqlTableOptionName.CONNECTION;}
| <DATA> <DIRECTORY> { return SqlTableOptionName.DATA_DIRECTORY;}
| <INDEX> <DIRECTORY> { return SqlTableOptionName.INDEX_DIRECTORY;}
| <DELAY_KEY_WRITE> { return SqlTableOptionName.DELAY_KEY_WRITE;}
| <ENCRYPTION> { return SqlTableOptionName.ENCRYPTION;}
| <ENGINE> { return SqlTableOptionName.ENGINE;}
| <ENGINE_ATTRIBUTE> { return SqlTableOptionName.ENGINE_ATTRIBUTE;}
| <INSERT_METHOD> { return SqlTableOptionName.INSERT_METHOD;}
| <KEY_BLOCK_SIZE> { return SqlTableOptionName.KEY_BLOCK_SIZE;}
| <MAX_ROWS> { return SqlTableOptionName.MAX_ROWS;}
| <MIN_ROWS> { return SqlTableOptionName.MIN_ROWS;}
| <PACK_KEYS> { return SqlTableOptionName.PACK_KEYS;}
| <PASSWORD> { return SqlTableOptionName.PASSWORD;}
| <ROW_FORMAT> { return SqlTableOptionName.ROW_FORMAT;}
| <SECONDARY_ENGINE_ATTRIBUTE> { return SqlTableOptionName.SECONDARY_ENGINE_ATTRIBUTE;}
| <STATS_AUTO_RECALC> { return SqlTableOptionName.STATS_AUTO_RECALC;}
| <STATS_PERSISTENT> { return SqlTableOptionName.STATS_PERSISTENT;}
| <STATS_SAMPLE_PAGES> { return SqlTableOptionName.STATS_SAMPLE_PAGES;}
| <TABLESPACE> { return SqlTableOptionName.TABLESPACE;}
| <UNION> { return SqlTableOptionName.UNION;}
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
    SqlNode comment = null;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    type = DataType()
    nullable = NullableOptDefaultTrue()
    (
        [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
        (
            <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
        |
            <STORED> { strategy = ColumnStrategy.STORED; }
        |
            { strategy = ColumnStrategy.VIRTUAL; }
        )
    |
        <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            strategy = ColumnStrategy.DEFAULT;
        }
    |
        {
            e = null;
            strategy = nullable ? ColumnStrategy.NULLABLE
                : ColumnStrategy.NOT_NULLABLE;
        }
    )
    [<COMMENT> comment = StringLiteral()]
    {
        list.add(
            SqlDdlNodes.column(s.add(id).end(this), id,
                type.withNullable(nullable), e, strategy, comment));
    }
|
    (
        (<INDEX>|<KEY>) { s.add(this); } name = SimpleIdentifier()
        columnList = ParenthesizedSimpleIdentifierList()
        [<COMMENT> comment = StringLiteral()]
        {
            list.add(SqlDdlNodes.index(s.end(columnList), name, columnList, comment));
        }
    )
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
        {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); } [<INDEX>|<KEY>]
        columnList = ParenthesizedSimpleIdentifierList()
        {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList()
        {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNodeList tableOptions = null;
    SqlPartitionOptions partitionOptions = null;
    SqlNode query = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ tableOptions = TableOptions() ]
    [ <PARTITION> partitionOptions = PartitionOptions()]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    {
        return SqlDdlNodes.createTable(s.end(this), replace, ifNotExists, id,
            tableElementList, tableOptions, partitionOptions, query);
    }
}

SqlPartitionOptions PartitionOptions() :
{
    final Span s;
    final Span s1;
    PartitionType type = null;
    SqlNode e = null;
    SqlNode alg = null;
    SqlNode num = null;
    SqlNodeList columnList = null;
    SqlNodeList partitions = null;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <BY> { s = span(); }
    (
        [ <LINEAR> ]
        (
            <HASH> {type = PartitionType.HASH;}
            <LPAREN> e = Expression(ExprContext.ACCEPT_NON_QUERY) <RPAREN>
        |
            <KEY> {type = PartitionType.KEY;}
            [ <ALGORITHM> <EQ> alg = Literal() ]
            <LPAREN> { s1 = span(); }
            [SimpleIdentifierCommaList(list)]
            <RPAREN>
            {
                columnList = new SqlNodeList(list, s1.end(this));
            }
        )
    |
        <RANGE> {type = PartitionType.RANGE;}
        (
            <LPAREN> e = Expression(ExprContext.ACCEPT_NON_QUERY) <RPAREN>
        |
            <COLUMNS> columnList = ParenthesizedSimpleIdentifierList()
        )
    |
        <LIST> {type = PartitionType.LIST;}
        (
            <LPAREN> e = Expression(ExprContext.ACCEPT_NON_QUERY) <RPAREN>
        |
            <COLUMNS> columnList = ParenthesizedSimpleIdentifierList()
        )
    )
    [ <PARTITIONS> num = Literal() ]
    [ partitions = Partitions() ]
    {
        return SqlDdlNodes.createPartitionOptions(type, e, alg, columnList, num, partitions, s.end(this));
    }
}

SqlNodeList Partitions() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    Partition(list)
    (
        <COMMA> Partition(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Partition(List<SqlNode> list) :
{
    final Span s;
    final Span s0;
    final Span s1;
    final Span s2;
    final SqlIdentifier name;
    SqlIdentifier type = null;
    SqlNode e = null;
    SqlNodeList valueList = null;
    SqlNode max = null;
    SqlIdentifier engineName = null;
    SqlNode comment = null;
    SqlNode dataDir = null;
    SqlNode indexDir = null;
    SqlNode maxRow = null;
    SqlNode minRow = null;
    SqlNode tablespace = null;
    final List<SqlNode> vl = new ArrayList<SqlNode>();
    SqlNode optionVal;
}
{
    <PARTITION> { s = span(); } name = SimpleIdentifier()
    [
        <VALUES>
        (
            <LESS> <THAN> { s0 = span(); } { type = new SqlIdentifier("LESS", s0.end(this)); }
            (
                { s2 = span(); }
                <LPAREN>
                (
                    optionVal = OptionValue()
                    {
                        vl.add(optionVal);
                    }
                    (
                        <COMMA>
                        optionVal = OptionValue()
                        {
                            vl.add(optionVal);
                        }
                    )*
                    { valueList = new SqlNodeList(vl, s2.end(this)); }
                |
                    <MAXVALUE> { optionVal = new SqlIdentifier("MAXVALUE", s2.end(this)); }
                    {
                        vl.add(optionVal);
                    }
                    (
                        <COMMA> <MAXVALUE>
                        { optionVal = new SqlIdentifier("MAXVALUE", s2.end(this)); }
                        {
                            vl.add(optionVal);
                        }
                    )*
                    { valueList = new SqlNodeList(vl, s2.end(this)); }
                |
                    (
                         e = Expression(ExprContext.ACCEPT_NON_QUERY)
                    )
                )
                <RPAREN>
            |
                [ <MAXVALUE> { s1 = span(); } { max = new SqlIdentifier("MAXVALUE", s1.end(this)); } ]
            )

        |
            <IN> { s0 = span(); } {type = new SqlIdentifier("IN", s0.end(this));}
            valueList = ParenthesizedLiteralOptionCommaList()
        )
    ]
    [ [<STORAGE>] <ENGINE> [<EQ>] engineName = SimpleIdentifier() ]
    [ <COMMENT> [<EQ>] comment = StringLiteral() ]
    [ <DATA> <DIRECTORY> [<EQ>] dataDir = StringLiteral() ]
    [ <INDEX> <DIRECTORY> [<EQ>] indexDir = StringLiteral() ]
    [ <MAX_ROWS> [<EQ>] maxRow = NumericLiteral() ]
    [ <MIN_ROWS> [<EQ>] minRow = NumericLiteral() ]
    [ <TABLESPACE> [<EQ>] tablespace = SimpleIdentifier() ]

    {
        list.add(SqlDdlNodes.createPartition(name, type, e, valueList, max, engineName,
                        comment, dataDir, indexDir, maxRow, minRow, tablespace, s.end(this)));
    }
}

SqlNodeList LiteralOptionCommaList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode optionVal;
}
{
    { s = span(); }
    optionVal = OptionValue()
    {
        list.add(optionVal);
    }
    (
        <COMMA>
        optionVal = OptionValue()
        {
            list.add(optionVal);
        }
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, columnList,
            query);
    }
}

SqlCreate SqlCreateMaterializedView(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <MATERIALIZED> <VIEW> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createMaterializedView(s.end(this), replace,
            ifNotExists, id, columnList, query);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
        <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
    |
        <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
    |
        <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), replace, ifNotExists,
            id, className, usingList);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final boolean foreign;
}
{
    (
        <FOREIGN> { foreign = true; }
    |
        { foreign = false; }
    )
    <SCHEMA> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), foreign, ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropMaterializedView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <MATERIALIZED> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropMaterializedView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}
