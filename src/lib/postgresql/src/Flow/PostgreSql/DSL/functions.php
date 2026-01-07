<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\PostgreSql\AST\Transformers\{CountModifier, ExplainConfig, ExplainModifier, KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, PaginationConfig, PaginationModifier, SortOrder};
use Flow\PostgreSql\Client;
use Flow\PostgreSql\Client\{ConnectionParameters, RowMapper, TypedValue};
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Exception\ConnectionException;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Client\RowMapper\ConstructorMapper;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverters};
use Flow\PostgreSql\{DeparseOptions, ParsedQuery, Parser};
use Flow\PostgreSql\Explain\Analyzer\PlanAnalyzer;
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Extractors\{Columns, Functions, OrderBy as OrderByExtractor, QueryDepth, Tables};
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Clause\{
    CTE,
    CTEMaterialization,
    ConflictTarget,
    FrameBound,
    FrameExclusion,
    FrameMode,
    LockStrength,
    LockWaitPolicy,
    LockingClause,
    NullsPosition,
    OnConflictClause,
    OrderBy,
    ReturningClause,
    SortDirection,
    WindowDefinition,
    WindowFrame
};
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\{
    All,
    AndCondition,
    Any,
    Between,
    Comparison,
    ComparisonOperator,
    Condition,
    ConditionBuilder,
    Exists,
    In,
    IsDistinctFrom,
    IsNull,
    Like,
    NotCondition,
    OperatorCondition,
    OrCondition,
    RawCondition,
    SimilarTo
};
use Flow\PostgreSql\QueryBuilder\Cursor\{
    CloseCursorBuilder,
    CloseCursorFinalStep,
    DeclareCursorBuilder,
    DeclareCursorOptionsStep,
    FetchCursorBuilder
};
use Flow\PostgreSql\QueryBuilder\Delete\{DeleteBuilder, DeleteFromStep};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\{
    AggregateCall,
    ArrayExpression,
    BinaryExpression,
    CaseExpression,
    Coalesce,
    Column,
    Expression,
    FunctionCall,
    Greatest,
    Least,
    Literal,
    NullIf,
    Parameter,
    RawExpression,
    RowExpression,
    SQLValueFunctionExpression,
    Star,
    Subquery,
    TypeCast,
    WhenClause,
    WindowFunction
};
use Flow\PostgreSql\QueryBuilder\Factory\{AlterFactory, CreateFactory, DropFactory};
use Flow\PostgreSql\QueryBuilder\Factory\CopyFactory;
use Flow\PostgreSql\QueryBuilder\Insert\{BulkInsert, InsertBuilder, InsertIntoStep};
use Flow\PostgreSql\QueryBuilder\Merge\{MergeBuilder, MergeUsingStep};
use Flow\PostgreSql\QueryBuilder\{QualifiedIdentifier, SqlQuery};
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, DataType, ReferentialAction};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\{CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint};
use Flow\PostgreSql\QueryBuilder\Schema\Function\{
    CallBuilder,
    CallFinalStep,
    DoBuilder,
    DoFinalStep,
    FunctionArgument
};
use Flow\PostgreSql\QueryBuilder\Schema\Grant\{
    GrantBuilder,
    GrantOnStep,
    GrantRoleBuilder,
    GrantRoleToStep,
    RevokeBuilder,
    RevokeOnStep,
    RevokeRoleBuilder,
    RevokeRoleFromStep,
    TablePrivilege
};
use Flow\PostgreSql\QueryBuilder\Schema\Index\{IndexColumn, IndexMethod};
use Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex\{ReindexBuilder, ReindexFinalStep};
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\{
    DropOwnedBuilder,
    DropOwnedFinalStep,
    ReassignOwnedBuilder,
    ReassignOwnedToStep
};
use Flow\PostgreSql\QueryBuilder\Schema\Session\{
    ResetRoleBuilder,
    ResetRoleFinalStep,
    SetRoleBuilder,
    SetRoleFinalStep
};
use Flow\PostgreSql\QueryBuilder\Schema\Truncate\{TruncateBuilder, TruncateFinalStep};
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView\{RefreshMatViewOptionsStep, RefreshMaterializedViewBuilder};
use Flow\PostgreSql\QueryBuilder\Select\{SelectBuilder, SelectFinalStep, SelectSelectStep};
use Flow\PostgreSql\QueryBuilder\Table\{
    DerivedTable,
    Lateral,
    Table,
    TableFunction,
    TableReference,
    ValuesTable
};
use Flow\PostgreSql\QueryBuilder\Transaction\{
    BeginBuilder,
    BeginOptionsStep,
    CommitBuilder,
    CommitOptionsStep,
    PreparedTransactionBuilder,
    PreparedTransactionFinalStep,
    RollbackBuilder,
    RollbackOptionsStep,
    SavepointBuilder,
    SavepointFinalStep,
    SetTransactionBuilder,
    SetTransactionFinalStep,
    SetTransactionOptionsStep
};
use Flow\PostgreSql\QueryBuilder\Update\{UpdateBuilder, UpdateTableStep};
use Flow\PostgreSql\QueryBuilder\Utility\{
    AnalyzeBuilder,
    AnalyzeFinalStep,
    ClusterBuilder,
    ClusterFinalStep,
    CommentBuilder,
    CommentFinalStep,
    CommentTarget,
    DiscardBuilder,
    DiscardFinalStep,
    DiscardType,
    ExplainBuilder,
    ExplainFinalStep,
    ExplainFormat,
    LockBuilder,
    LockFinalStep,
    VacuumBuilder,
    VacuumFinalStep
};
use Flow\PostgreSql\QueryBuilder\With\WithBuilder;

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_parser() : Parser
{
    return new Parser();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_parse(string $sql) : ParsedQuery
{
    return (new Parser())->parse($sql);
}

/**
 * Returns a fingerprint of the given SQL query.
 * Literal values are normalized so they won't affect the fingerprint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_fingerprint(string $sql) : ?string
{
    return (new Parser())->fingerprint($sql);
}

/**
 * Normalize SQL query by replacing literal values and named parameters with positional parameters.
 * WHERE id = :id will be changed into WHERE id = $1
 * WHERE id = 1 will be changed into WHERE id = $1.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_normalize(string $sql) : ?string
{
    return (new Parser())->normalize($sql);
}

/**
 * Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).
 * This handles DDL statements differently from pg_normalize() which is optimized for DML.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_normalize_utility(string $sql) : ?string
{
    return (new Parser())->normalizeUtility($sql);
}

/**
 * Split string with multiple SQL statements into array of individual statements.
 *
 * @return array<string>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_split(string $sql) : array
{
    return (new Parser())->split($sql);
}

/**
 * Create DeparseOptions for configuring SQL formatting.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_deparse_options() : DeparseOptions
{
    return DeparseOptions::new();
}

/**
 * Convert a ParsedQuery AST back to SQL string.
 *
 * When called without options, returns the SQL as a simple string.
 * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
 *
 * @throws \RuntimeException if deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_deparse(ParsedQuery $query, ?DeparseOptions $options = null) : string
{
    return $query->deparse($options);
}

/**
 * Parse and format SQL query with pretty printing.
 *
 * This is a convenience function that parses SQL and returns it formatted.
 *
 * @param string $sql The SQL query to format
 * @param null|DeparseOptions $options Formatting options (defaults to pretty-print enabled)
 *
 * @throws \RuntimeException if parsing or deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_format(string $sql, ?DeparseOptions $options = null) : string
{
    return (new Parser())->parse($sql)->deparse($options ?? DeparseOptions::new());
}

/**
 * Generate a summary of parsed queries in protobuf format.
 * Useful for query monitoring and logging without full AST overhead.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_summary(string $sql, int $options = 0, int $truncateLimit = 0) : string
{
    return (new Parser())->summary($sql, $options, $truncateLimit);
}

/**
 * Transform a SQL query into a paginated query with LIMIT and OFFSET.
 *
 * @param string $sql The SQL query to paginate
 * @param int $limit Maximum number of rows to return
 * @param int $offset Number of rows to skip (requires ORDER BY in query)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_paginated_query(string $sql, int $limit, int $offset = 0) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new PaginationModifier(new PaginationConfig($limit, $offset)));

    return $query->deparse();
}

/**
 * Transform a SQL query to limit results to a specific number of rows.
 *
 * @param string $sql The SQL query to limit
 * @param int $limit Maximum number of rows to return
 *
 * @return string The limited SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_limited_query(string $sql, int $limit) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new PaginationModifier(new PaginationConfig($limit)));

    return $query->deparse();
}

/**
 * Transform a SQL query into a COUNT query for pagination.
 *
 * Wraps the query in: SELECT COUNT(*) FROM (...) AS _count_subq
 * Removes ORDER BY and LIMIT/OFFSET from the inner query.
 *
 * @param string $sql The SQL query to transform
 *
 * @return string The COUNT query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_count_query(string $sql) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new CountModifier());

    return $query->deparse();
}

/**
 * Transform a SQL query into a keyset (cursor-based) paginated query.
 *
 * More efficient than OFFSET for large datasets - uses indexed WHERE conditions.
 * Automatically detects existing query parameters and appends keyset placeholders at the end.
 *
 * @param string $sql The SQL query to paginate (must have ORDER BY)
 * @param int $limit Maximum number of rows to return
 * @param list<KeysetColumn> $columns Columns for keyset pagination (must match ORDER BY)
 * @param null|list<null|bool|float|int|string> $cursor Values from last row of previous page (null for first page)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_keyset_query(string $sql, int $limit, array $columns, ?array $cursor = null) : string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new KeysetPaginationModifier(new KeysetPaginationConfig($limit, $columns, $cursor)));

    return $query->deparse();
}

/**
 * Create a KeysetColumn for keyset pagination.
 *
 * @param string $column Column name (can include table alias like "u.id")
 * @param SortOrder $order Sort order (ASC or DESC)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_keyset_column(string $column, SortOrder $order = SortOrder::ASC) : KeysetColumn
{
    return new KeysetColumn($column, $order);
}

/**
 * Extract columns from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_columns(ParsedQuery $query) : Columns
{
    return new Columns($query);
}

/**
 * Extract tables from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_tables(ParsedQuery $query) : Tables
{
    return new Tables($query);
}

/**
 * Extract functions from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_functions(ParsedQuery $query) : Functions
{
    return new Functions($query);
}

/**
 * Extract ORDER BY clauses from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_order_by(ParsedQuery $query) : OrderByExtractor
{
    return new OrderByExtractor($query);
}

/**
 * Get the maximum nesting depth of a SQL query.
 *
 * Example:
 * - "SELECT * FROM t" => 1
 * - "SELECT * FROM (SELECT * FROM t)" => 2
 * - "SELECT * FROM (SELECT * FROM (SELECT * FROM t))" => 3
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_depth(string $sql) : int
{
    return (new QueryDepth(sql_parse($sql)))->depth();
}

/**
 * Transform a SQL query into an EXPLAIN query.
 *
 * Returns the modified SQL with EXPLAIN wrapped around it.
 * Defaults to EXPLAIN ANALYZE with JSON format for easy parsing.
 *
 * @param string $sql The SQL query to explain
 * @param null|ExplainConfig $config EXPLAIN configuration (defaults to forAnalysis())
 *
 * @return string The EXPLAIN query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_explain(string $sql, ?ExplainConfig $config = null) : string
{
    $config ??= ExplainConfig::forAnalysis();
    $query = (new Parser())->parse($sql);
    $query->traverse(new ExplainModifier($config));

    return $query->deparse();
}

/**
 * Create an ExplainConfig for customizing EXPLAIN options.
 *
 * @param bool $analyze Whether to actually execute the query (ANALYZE)
 * @param bool $verbose Include verbose output
 * @param bool $costs Include cost estimates (default true)
 * @param bool $buffers Include buffer usage statistics (requires analyze)
 * @param bool $timing Include timing information (requires analyze)
 * @param ExplainFormat $format Output format (JSON recommended for parsing)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_config(
    bool $analyze = true,
    bool $verbose = false,
    bool $costs = true,
    bool $buffers = true,
    bool $timing = true,
    ExplainFormat $format = ExplainFormat::JSON,
) : ExplainConfig {
    return new ExplainConfig(
        analyze: $analyze,
        verbose: $verbose,
        costs: $costs,
        buffers: $buffers,
        timing: $timing,
        format: $format,
    );
}

/**
 * Create an ExplainModifier for transforming queries into EXPLAIN queries.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_modifier(ExplainConfig $config) : ExplainModifier
{
    return new ExplainModifier($config);
}

/**
 * Parse EXPLAIN JSON output into a Plan object.
 *
 * @param string $jsonOutput The JSON output from EXPLAIN (FORMAT JSON)
 *
 * @return Plan The parsed execution plan
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_parse(string $jsonOutput) : Plan
{
    return (new ExplainParser())->parse($jsonOutput);
}

/**
 * Create a plan analyzer for analyzing EXPLAIN plans.
 *
 * @param Plan $plan The execution plan to analyze
 *
 * @return PlanAnalyzer The analyzer for extracting insights
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_analyze(Plan $plan) : PlanAnalyzer
{
    return new PlanAnalyzer($plan);
}

/**
 * Create a new SELECT query builder.
 *
 * @param Expression ...$expressions Columns to select. If empty, returns SelectSelectStep.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function select(Expression ...$expressions) : SelectBuilder
{
    if ($expressions === []) {
        return SelectBuilder::create();
    }

    return SelectBuilder::create()->select(...$expressions);
}

/**
 * Create a WITH clause builder for CTEs.
 *
 * Example: with(cte('users', $subquery))->select(star())->from(table('users'))
 * Example: with(cte('a', $q1), cte('b', $q2))->recursive()->select(...)->from(table('a'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function with(CTE ...$ctes) : WithBuilder
{
    if ($ctes === []) {
        throw new \InvalidArgumentException('At least one CTE is required');
    }

    return new WithBuilder(new WithClause($ctes));
}

/**
 * Create a new INSERT query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function insert() : InsertIntoStep
{
    return InsertBuilder::create();
}

/**
 * Create an optimized bulk INSERT query for high-performance multi-row inserts.
 *
 * Unlike insert() which uses immutable builder patterns (O(n²) for n rows),
 * this function generates SQL directly using string operations (O(n) complexity).
 *
 * @param string $table Table name
 * @param list<string> $columns Column names
 * @param int $rowCount Number of rows to insert
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function bulk_insert(string $table, array $columns, int $rowCount) : BulkInsert
{
    return BulkInsert::into($table, $columns, $rowCount);
}

/**
 * Create a new UPDATE query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function update() : UpdateTableStep
{
    return UpdateBuilder::create();
}

/**
 * Create a new DELETE query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function delete() : DeleteFromStep
{
    return DeleteBuilder::create();
}

/**
 * Create a new MERGE query builder.
 *
 * @param string $table Target table name
 * @param null|string $alias Optional table alias
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function merge(string $table, ?string $alias = null) : MergeUsingStep
{
    return MergeBuilder::create()->into($table, $alias);
}

/**
 * Create a new COPY query builder for data import/export.
 *
 * Usage:
 *   copy()->from('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)
 *   copy()->to('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)
 *   copy()->toQuery(select(...))->file('/tmp/data.csv')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function copy() : CopyFactory
{
    return new CopyFactory();
}

/**
 * Create a column reference expression.
 *
 * Can be used in two modes:
 * - Parse mode: col('users.id') or col('schema.table.column') - parses dot-separated string
 * - Explicit mode: col('id', 'users') or col('id', 'users', 'schema') - separate arguments
 *
 * When $table or $schema is provided, $column must be a plain column name (no dots).
 *
 * @param string $column Column name, or dot-separated path like "table.column" or "schema.table.column"
 * @param null|string $table Table name (optional, triggers explicit mode)
 * @param null|string $schema Schema name (optional, requires $table)
 *
 * @throws InvalidExpressionException when $schema is provided without $table, or when $column contains dots in explicit mode
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function col(string $column, ?string $table = null, ?string $schema = null) : Column
{
    if ($table !== null || $schema !== null) {
        if ($schema !== null && $table === null) {
            throw new InvalidExpressionException('Cannot specify schema without table in col()');
        }

        if (\str_contains($column, '.')) {
            throw new InvalidExpressionException('Column name cannot contain dots when table or schema is specified. Use col("table.column") or col("column", "table") but not both.');
        }

        if ($schema !== null && $table !== null) {
            return Column::schemaTableColumn($schema, $table, $column);
        }

        if ($table === null) {
            return Column::fromParts(QualifiedIdentifier::parse($column)->parts());
        }

        return Column::tableColumn($table, $column);
    }

    return Column::fromParts(QualifiedIdentifier::parse($column)->parts());
}

/**
 * Create a SELECT * expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function star(?string $table = null) : Star
{
    return $table !== null ? Star::fromTable($table) : Star::all();
}

/**
 * Create a literal value for use in queries.
 *
 * Automatically detects the type and creates the appropriate literal:
 * - literal('hello') creates a string literal
 * - literal(42) creates an integer literal
 * - literal(3.14) creates a float literal
 * - literal(true) creates a boolean literal
 * - literal(null) creates a NULL literal
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal(string|int|float|bool|null $value) : Literal
{
    return match (true) {
        $value === null => Literal::null(),
        \is_string($value) => Literal::string($value),
        \is_int($value) => Literal::int($value),
        \is_float($value) => Literal::float($value),
        \is_bool($value) => Literal::bool($value),
    };
}

/**
 * Create a positional parameter ($1, $2, etc.).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function param(int $position) : Parameter
{
    return Parameter::positional($position);
}

/**
 * Create a function call expression.
 *
 * @param string $name Function name (can include schema like "pg_catalog.now")
 * @param list<Expression> $args Function arguments
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function func(string $name, array $args = []) : FunctionCall
{
    return new FunctionCall(QualifiedIdentifier::parse($name)->parts(), $args);
}

/**
 * Create an aggregate function call (COUNT, SUM, AVG, etc.).
 *
 * @param string $name Aggregate function name
 * @param list<Expression> $args Function arguments
 * @param bool $distinct Use DISTINCT modifier
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg(string $name, array $args = [], bool $distinct = false) : AggregateCall
{
    return new AggregateCall([$name], $args, false, $distinct);
}

/**
 * Create COUNT(*) aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_count(?Expression $expr = null, bool $distinct = false) : AggregateCall
{
    if ($expr === null) {
        return new AggregateCall(['count'], [], true, false);
    }

    return new AggregateCall(['count'], [$expr], false, $distinct);
}

/**
 * Create SUM aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_sum(Expression $expr, bool $distinct = false) : AggregateCall
{
    return new AggregateCall(['sum'], [$expr], false, $distinct);
}

/**
 * Create AVG aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_avg(Expression $expr, bool $distinct = false) : AggregateCall
{
    return new AggregateCall(['avg'], [$expr], false, $distinct);
}

/**
 * Create MIN aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_min(Expression $expr) : AggregateCall
{
    return new AggregateCall(['min'], [$expr]);
}

/**
 * Create MAX aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_max(Expression $expr) : AggregateCall
{
    return new AggregateCall(['max'], [$expr]);
}

/**
 * Create a COALESCE expression.
 *
 * @param Expression ...$expressions Expressions to coalesce
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function coalesce(Expression ...$expressions) : Coalesce
{
    return new Coalesce(\array_values($expressions));
}

/**
 * Create a NULLIF expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function nullif(Expression $expr1, Expression $expr2) : NullIf
{
    return new NullIf($expr1, $expr2);
}

/**
 * Create a GREATEST expression.
 *
 * @param Expression ...$expressions Expressions to compare
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function greatest(Expression ...$expressions) : Greatest
{
    return new Greatest(\array_values($expressions));
}

/**
 * Create a LEAST expression.
 *
 * @param Expression ...$expressions Expressions to compare
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function least(Expression ...$expressions) : Least
{
    return new Least(\array_values($expressions));
}

/**
 * Create a type cast expression.
 *
 * @param Expression $expr Expression to cast
 * @param DataType $dataType Target data type (use data_type_* functions)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cast(Expression $expr, DataType $dataType) : TypeCast
{
    return new TypeCast($expr, $dataType);
}

/**
 * Create an integer data type (PostgreSQL int4).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_integer() : DataType
{
    return DataType::integer();
}

/**
 * Create a smallint data type (PostgreSQL int2).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_smallint() : DataType
{
    return DataType::smallint();
}

/**
 * Create a bigint data type (PostgreSQL int8).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_bigint() : DataType
{
    return DataType::bigint();
}

/**
 * Create a boolean data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_boolean() : DataType
{
    return DataType::boolean();
}

/**
 * Create a text data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_text() : DataType
{
    return DataType::text();
}

/**
 * Create a varchar data type with length constraint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_varchar(int $length) : DataType
{
    return DataType::varchar($length);
}

/**
 * Create a char data type with length constraint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_char(int $length) : DataType
{
    return DataType::char($length);
}

/**
 * Create a numeric data type with optional precision and scale.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_numeric(?int $precision = null, ?int $scale = null) : DataType
{
    return DataType::numeric($precision, $scale);
}

/**
 * Create a decimal data type with optional precision and scale.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_decimal(?int $precision = null, ?int $scale = null) : DataType
{
    return DataType::decimal($precision, $scale);
}

/**
 * Create a real data type (PostgreSQL float4).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_real() : DataType
{
    return DataType::real();
}

/**
 * Create a double precision data type (PostgreSQL float8).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_double_precision() : DataType
{
    return DataType::doublePrecision();
}

/**
 * Create a date data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_date() : DataType
{
    return DataType::date();
}

/**
 * Create a time data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_time(?int $precision = null) : DataType
{
    return DataType::time($precision);
}

/**
 * Create a timestamp data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_timestamp(?int $precision = null) : DataType
{
    return DataType::timestamp($precision);
}

/**
 * Create a timestamp with time zone data type with optional precision.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_timestamptz(?int $precision = null) : DataType
{
    return DataType::timestamptz($precision);
}

/**
 * Create an interval data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_interval() : DataType
{
    return DataType::interval();
}

/**
 * SQL standard CURRENT_TIMESTAMP function.
 *
 * Returns the current date and time (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('created_at', data_type_timestamp())->default(current_timestamp())
 * Example: select()->select(current_timestamp()->as('now'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_timestamp() : SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentTimestamp();
}

/**
 * SQL standard CURRENT_DATE function.
 *
 * Returns the current date (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('birth_date', data_type_date())->default(current_date())
 * Example: select()->select(current_date()->as('today'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_date() : SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentDate();
}

/**
 * SQL standard CURRENT_TIME function.
 *
 * Returns the current time (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('start_time', data_type_time())->default(current_time())
 * Example: select()->select(current_time()->as('now_time'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_time() : SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentTime();
}

/**
 * Create a UUID data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_uuid() : DataType
{
    return DataType::uuid();
}

/**
 * Create a JSON data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_json() : DataType
{
    return DataType::json();
}

/**
 * Create a JSONB data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_jsonb() : DataType
{
    return DataType::jsonb();
}

/**
 * Create a bytea data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_bytea() : DataType
{
    return DataType::bytea();
}

/**
 * Create an inet data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_inet() : DataType
{
    return DataType::inet();
}

/**
 * Create a cidr data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_cidr() : DataType
{
    return DataType::cidr();
}

/**
 * Create a macaddr data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_macaddr() : DataType
{
    return DataType::macaddr();
}

/**
 * Create a serial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_serial() : DataType
{
    return DataType::serial();
}

/**
 * Create a smallserial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_smallserial() : DataType
{
    return DataType::smallserial();
}

/**
 * Create a bigserial data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_bigserial() : DataType
{
    return DataType::bigserial();
}

/**
 * Create an array data type from an element type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_array(DataType $elementType) : DataType
{
    return DataType::array($elementType);
}

/**
 * Create a custom data type.
 *
 * @param string $typeName Type name
 * @param null|string $schema Optional schema name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function data_type_custom(string $typeName, ?string $schema = null) : DataType
{
    return DataType::custom($typeName, $schema);
}

/**
 * Create a CASE expression.
 *
 * @param non-empty-list<WhenClause> $whenClauses WHEN clauses
 * @param null|Expression $elseResult ELSE result (optional)
 * @param null|Expression $operand CASE operand for simple CASE (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function case_when(array $whenClauses, ?Expression $elseResult = null, ?Expression $operand = null) : CaseExpression
{
    return new CaseExpression($operand, \array_values($whenClauses), $elseResult);
}

/**
 * Create a WHEN clause for CASE expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function when(Expression $condition, Expression $result) : WhenClause
{
    return new WhenClause($condition, $result);
}

/**
 * Create a subquery expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sub_select(SelectFinalStep $query) : Subquery
{
    $node = new Node();
    $node->setSelectStmt($query->toAst());

    return new Subquery($node);
}

/**
 * Create an array expression.
 *
 * @param list<Expression> $elements Array elements
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_expr(array $elements) : ArrayExpression
{
    return new ArrayExpression(\array_values($elements));
}

/**
 * Create a row expression.
 *
 * @param list<Expression> $elements Row elements
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function row_expr(array $elements) : RowExpression
{
    return new RowExpression(\array_values($elements));
}

/**
 * Create a raw SQL expression (use with caution).
 *
 * SECURITY WARNING: This function accepts raw SQL without parameterization.
 * SQL injection is possible if used with untrusted user input.
 * Only use with trusted, validated input.
 *
 * For user-provided values, use param() instead:
 * ```php
 * // UNSAFE - SQL injection possible:
 * raw_expr("custom_func('" . $userInput . "')")
 *
 * // SAFE - use parameters:
 * func('custom_func', param(1))
 * ```
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function raw_expr(string $sql) : RawExpression
{
    return new RawExpression($sql);
}

/**
 * Create a binary expression (left op right).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function binary_expr(Expression $left, string $operator, Expression $right) : BinaryExpression
{
    return new BinaryExpression($left, $operator, $right);
}

/**
 * Create a window function.
 *
 * @param string $name Function name
 * @param list<Expression> $args Function arguments
 * @param list<Expression> $partitionBy PARTITION BY expressions
 * @param list<OrderBy> $orderBy ORDER BY items
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function window_func(
    string $name,
    array $args = [],
    array $partitionBy = [],
    array $orderBy = [],
) : WindowFunction {
    return new WindowFunction([$name], \array_values($args), \array_values($partitionBy), \array_values($orderBy));
}

/**
 * Create an equality comparison (column = value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function eq(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::EQ, $right);
}

/**
 * Create a not-equal comparison (column != value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function neq(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::NEQ, $right);
}

/**
 * Create a less-than comparison (column < value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lt(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::LT, $right);
}

/**
 * Create a less-than-or-equal comparison (column <= value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lte(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::LTE, $right);
}

/**
 * Create a greater-than comparison (column > value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function gt(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::GT, $right);
}

/**
 * Create a greater-than-or-equal comparison (column >= value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function gte(Expression $left, Expression $right) : Comparison
{
    return new Comparison($left, ComparisonOperator::GTE, $right);
}

/**
 * Create a BETWEEN condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function between(Expression $expr, Expression $low, Expression $high, bool $not = false) : Between
{
    return new Between($expr, $low, $high, $not);
}

/**
 * Create an IN condition.
 *
 * @param Expression $expr Expression to check
 * @param list<Expression> $values List of values
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function is_in(Expression $expr, array $values) : In
{
    return new In($expr, \array_values($values));
}

/**
 * Create an IS NULL condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function is_null(Expression $expr, bool $not = false) : IsNull
{
    return new IsNull($expr, $not);
}

/**
 * Create a LIKE condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function like(Expression $expr, Expression $pattern, bool $caseInsensitive = false) : Like
{
    return new Like($expr, $pattern, $caseInsensitive);
}

/**
 * Create a SIMILAR TO condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function similar_to(Expression $expr, Expression $pattern) : SimilarTo
{
    return new SimilarTo($expr, $pattern);
}

/**
 * Create an IS DISTINCT FROM condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function is_distinct_from(Expression $left, Expression $right, bool $not = false) : IsDistinctFrom
{
    return new IsDistinctFrom($left, $right, $not);
}

/**
 * Create an EXISTS condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exists(SelectFinalStep $subquery) : Exists
{
    $node = new Node();
    $node->setSelectStmt($subquery->toAst());

    return new Exists($node);
}

/**
 * Create an ANY condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function any_sub_select(Expression $left, ComparisonOperator $operator, SelectFinalStep $subquery) : Any
{
    $node = new Node();
    $node->setSelectStmt($subquery->toAst());

    return new Any($left, $operator, $node);
}

/**
 * Create an ALL condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function all_sub_select(Expression $left, ComparisonOperator $operator, SelectFinalStep $subquery) : All
{
    $node = new Node();
    $node->setSelectStmt($subquery->toAst());

    return new All($left, $operator, $node);
}

/**
 * Create a condition builder for fluent condition composition.
 *
 * This builder allows incremental condition building with a fluent API:
 *
 * ```php
 * $builder = conditions();
 *
 * if ($hasFilter) {
 *     $builder = $builder->and(eq(col('status'), literal('active')));
 * }
 *
 * if (!$builder->isEmpty()) {
 *     $query = select()->from(table('users'))->where($builder);
 * }
 * ```
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function conditions() : ConditionBuilder
{
    return ConditionBuilder::create();
}

/**
 * Combine conditions with AND.
 *
 * @param Condition ...$conditions Conditions to combine
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cond_and(Condition ...$conditions) : AndCondition
{
    return new AndCondition(...$conditions);
}

/**
 * Combine conditions with OR.
 *
 * @param Condition ...$conditions Conditions to combine
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cond_or(Condition ...$conditions) : OrCondition
{
    return new OrCondition(...$conditions);
}

/**
 * Negate a condition with NOT.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cond_not(Condition $condition) : NotCondition
{
    return new NotCondition($condition);
}

/**
 * Create a raw SQL condition (use with caution).
 *
 * SECURITY WARNING: This function accepts raw SQL without parameterization.
 * SQL injection is possible if used with untrusted user input.
 * Only use with trusted, validated input.
 *
 * For user-provided values, use standard condition functions with param():
 * ```php
 * // UNSAFE - SQL injection possible:
 * raw_cond("status = '" . $userInput . "'")
 *
 * // SAFE - use typed conditions:
 * eq(col('status'), param(1))
 * ```
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function raw_cond(string $sql) : RawCondition
{
    return new RawCondition($sql);
}

/**
 * Create a TRUE condition for WHERE clauses.
 *
 * Useful when you need a condition that always evaluates to true.
 *
 * Example: select(literal(1))->where(cond_true()) // SELECT 1 WHERE true
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cond_true() : RawCondition
{
    return new RawCondition('true');
}

/**
 * Create a FALSE condition for WHERE clauses.
 *
 * Useful when you need a condition that always evaluates to false,
 * typically for testing or to return an empty result set.
 *
 * Example: select(literal(1))->where(cond_false()) // SELECT 1 WHERE false
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cond_false() : RawCondition
{
    return new RawCondition('false');
}

/**
 * Create a JSONB contains condition (@>).
 *
 * Example: json_contains(col('metadata'), literal_json('{"category": "electronics"}'))
 * Produces: metadata @> '{"category": "electronics"}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_contains(Expression $left, Expression $right) : OperatorCondition
{
    return new OperatorCondition($left, '@>', $right);
}

/**
 * Create a JSONB is contained by condition (<@).
 *
 * Example: json_contained_by(col('metadata'), literal_json('{"category": "electronics", "price": 100}'))
 * Produces: metadata <@ '{"category": "electronics", "price": 100}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_contained_by(Expression $left, Expression $right) : OperatorCondition
{
    return new OperatorCondition($left, '<@', $right);
}

/**
 * Create a JSON field access expression (->).
 * Returns JSON.
 *
 * Example: json_get(col('metadata'), literal_string('category'))
 * Produces: metadata -> 'category'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_get(Expression $expr, Expression $key) : BinaryExpression
{
    return new BinaryExpression($expr, '->', $key);
}

/**
 * Create a JSON field access expression (->>).
 * Returns text.
 *
 * Example: json_get_text(col('metadata'), literal_string('name'))
 * Produces: metadata ->> 'name'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_get_text(Expression $expr, Expression $key) : BinaryExpression
{
    return new BinaryExpression($expr, '->>', $key);
}

/**
 * Create a JSON path access expression (#>).
 * Returns JSON.
 *
 * Example: json_path(col('metadata'), literal_string('{category,name}'))
 * Produces: metadata #> '{category,name}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_path(Expression $expr, Expression $path) : BinaryExpression
{
    return new BinaryExpression($expr, '#>', $path);
}

/**
 * Create a JSON path access expression (#>>).
 * Returns text.
 *
 * Example: json_path_text(col('metadata'), literal_string('{category,name}'))
 * Produces: metadata #>> '{category,name}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_path_text(Expression $expr, Expression $path) : BinaryExpression
{
    return new BinaryExpression($expr, '#>>', $path);
}

/**
 * Create a JSONB key exists condition (?).
 *
 * Example: json_exists(col('metadata'), literal_string('category'))
 * Produces: metadata ? 'category'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists(Expression $expr, Expression $key) : OperatorCondition
{
    return new OperatorCondition($expr, '?', $key);
}

/**
 * Create a JSONB any key exists condition (?|).
 *
 * Example: json_exists_any(col('metadata'), raw_expr("array['category', 'name']"))
 * Produces: metadata ?| array['category', 'name']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists_any(Expression $expr, Expression $keys) : OperatorCondition
{
    return new OperatorCondition($expr, '?|', $keys);
}

/**
 * Create a JSONB all keys exist condition (?&).
 *
 * Example: json_exists_all(col('metadata'), raw_expr("array['category', 'name']"))
 * Produces: metadata ?& array['category', 'name']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists_all(Expression $expr, Expression $keys) : OperatorCondition
{
    return new OperatorCondition($expr, '?&', $keys);
}

/**
 * Create an array contains condition (@>).
 *
 * Example: array_contains(col('tags'), raw_expr("ARRAY['sale']"))
 * Produces: tags @> ARRAY['sale']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_contains(Expression $left, Expression $right) : OperatorCondition
{
    return new OperatorCondition($left, '@>', $right);
}

/**
 * Create an array is contained by condition (<@).
 *
 * Example: array_contained_by(col('tags'), raw_expr("ARRAY['sale', 'featured', 'new']"))
 * Produces: tags <@ ARRAY['sale', 'featured', 'new']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_contained_by(Expression $left, Expression $right) : OperatorCondition
{
    return new OperatorCondition($left, '<@', $right);
}

/**
 * Create an array overlap condition (&&).
 *
 * Example: array_overlap(col('tags'), raw_expr("ARRAY['sale', 'featured']"))
 * Produces: tags && ARRAY['sale', 'featured']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_overlap(Expression $left, Expression $right) : OperatorCondition
{
    return new OperatorCondition($left, '&&', $right);
}

/**
 * Create a POSIX regex match condition (~).
 * Case-sensitive.
 *
 * Example: regex_match(col('email'), literal_string('.*@gmail\\.com'))
 *
 * Produces: email ~ '.*@gmail\.com'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function regex_match(Expression $expr, Expression $pattern) : OperatorCondition
{
    return new OperatorCondition($expr, '~', $pattern);
}

/**
 * Create a POSIX regex match condition (~*).
 * Case-insensitive.
 *
 * Example: regex_imatch(col('email'), literal_string('.*@gmail\\.com'))
 *
 * Produces: email ~* '.*@gmail\.com'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function regex_imatch(Expression $expr, Expression $pattern) : OperatorCondition
{
    return new OperatorCondition($expr, '~*', $pattern);
}

/**
 * Create a POSIX regex not match condition (!~).
 * Case-sensitive.
 *
 * Example: not_regex_match(col('email'), literal_string('.*@spam\\.com'))
 *
 * Produces: email !~ '.*@spam\.com'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function not_regex_match(Expression $expr, Expression $pattern) : OperatorCondition
{
    return new OperatorCondition($expr, '!~', $pattern);
}

/**
 * Create a POSIX regex not match condition (!~*).
 * Case-insensitive.
 *
 * Example: not_regex_imatch(col('email'), literal_string('.*@spam\\.com'))
 *
 * Produces: email !~* '.*@spam\.com'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function not_regex_imatch(Expression $expr, Expression $pattern) : OperatorCondition
{
    return new OperatorCondition($expr, '!~*', $pattern);
}

/**
 * Create a full-text search match condition (@@).
 *
 * Example: text_search_match(col('document'), raw_expr("to_tsquery('english', 'hello & world')"))
 * Produces: document @@ to_tsquery('english', 'hello & world')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function text_search_match(Expression $document, Expression $query) : OperatorCondition
{
    return new OperatorCondition($document, '@@', $query);
}

/**
 * Create a table reference.
 *
 * Supports dot notation for schema-qualified names: "public.users" or explicit schema parameter.
 * Double-quoted identifiers preserve dots: '"my.table"' creates a single identifier.
 *
 * @param string $name Table name (may include schema as "schema.table")
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function table(string $name, ?string $schema = null) : Table
{
    if ($schema !== null) {
        return new Table($name, $schema);
    }

    $identifier = QualifiedIdentifier::parse($name);

    return new Table($identifier->name(), $identifier->schema());
}

/**
 * Create a derived table (subquery in FROM clause).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function derived(SelectFinalStep $query, string $alias) : DerivedTable
{
    $node = new Node();
    $node->setSelectStmt($query->toAst());

    return new DerivedTable($node, $alias);
}

/**
 * Create a LATERAL subquery.
 *
 * @param TableReference $reference The subquery or table function reference
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lateral(TableReference $reference) : Lateral
{
    return new Lateral($reference);
}

/**
 * Create a table function reference.
 *
 * @param FunctionCall $function The table-valued function
 * @param bool $withOrdinality Whether to add WITH ORDINALITY
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function table_func(FunctionCall $function, bool $withOrdinality = false) : TableFunction
{
    return new TableFunction($function, $withOrdinality);
}

/**
 * Create a VALUES clause as a table reference.
 *
 * Usage:
 *   select()->from(
 *       values_table(
 *           row_expr([literal(1), literal('Alice')]),
 *           row_expr([literal(2), literal('Bob')])
 *       )->as('t', ['id', 'name'])
 *   )
 *
 * Generates: SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function values_table(RowExpression ...$rows) : ValuesTable
{
    return new ValuesTable($rows);
}

/**
 * Create an ORDER BY item.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function order_by(
    Expression $expr,
    SortDirection $direction = SortDirection::ASC,
    NullsPosition $nulls = NullsPosition::DEFAULT,
) : OrderBy {
    return new OrderBy($expr, $direction, $nulls);
}

/**
 * Create an ORDER BY item with ASC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function asc(Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT) : OrderBy
{
    return new OrderBy($expr, SortDirection::ASC, $nulls);
}

/**
 * Create an ORDER BY item with DESC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function desc(Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT) : OrderBy
{
    return new OrderBy($expr, SortDirection::DESC, $nulls);
}

/**
 * Create a CTE (Common Table Expression).
 *
 * @param string $name CTE name
 * @param SelectFinalStep $query CTE query
 * @param array<string> $columnNames Column aliases (optional)
 * @param CTEMaterialization $materialization Materialization hint
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cte(
    string $name,
    SelectFinalStep $query,
    array $columnNames = [],
    CTEMaterialization $materialization = CTEMaterialization::DEFAULT,
    bool $recursive = false,
) : CTE {
    $node = new Node();
    $node->setSelectStmt($query->toAst());

    return new CTE($name, $node, $columnNames, $materialization, $recursive);
}

/**
 * Create a window definition for WINDOW clause.
 *
 * @param string $name Window name
 * @param list<Expression> $partitionBy PARTITION BY expressions
 * @param list<OrderBy> $orderBy ORDER BY items
 * @param null|WindowFrame $frame Window frame specification
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function window_def(
    string $name,
    array $partitionBy = [],
    array $orderBy = [],
    ?WindowFrame $frame = null,
) : WindowDefinition {
    return new WindowDefinition($name, \array_values($partitionBy), \array_values($orderBy), $frame);
}

/**
 * Create a window frame specification.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function window_frame(
    FrameMode $mode,
    FrameBound $start,
    ?FrameBound $end = null,
    FrameExclusion $exclusion = FrameExclusion::NO_OTHERS,
) : WindowFrame {
    return new WindowFrame($mode, $start, $end, $exclusion);
}

/**
 * Create a frame bound for CURRENT ROW.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_current_row() : FrameBound
{
    return FrameBound::currentRow();
}

/**
 * Create a frame bound for UNBOUNDED PRECEDING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_unbounded_preceding() : FrameBound
{
    return FrameBound::unboundedPreceding();
}

/**
 * Create a frame bound for UNBOUNDED FOLLOWING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_unbounded_following() : FrameBound
{
    return FrameBound::unboundedFollowing();
}

/**
 * Create a frame bound for N PRECEDING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_preceding(Expression $offset) : FrameBound
{
    return FrameBound::preceding($offset);
}

/**
 * Create a frame bound for N FOLLOWING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_following(Expression $offset) : FrameBound
{
    return FrameBound::following($offset);
}

/**
 * Create a locking clause (FOR UPDATE, FOR SHARE, etc.).
 *
 * @param LockStrength $strength Lock strength
 * @param list<string> $tables Tables to lock (empty for all)
 * @param LockWaitPolicy $waitPolicy Wait policy
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lock_for(
    LockStrength $strength,
    array $tables = [],
    LockWaitPolicy $waitPolicy = LockWaitPolicy::DEFAULT,
) : LockingClause {
    return new LockingClause($strength, \array_values($tables), $waitPolicy);
}

/**
 * Create a FOR UPDATE locking clause.
 *
 * @param list<string> $tables Tables to lock (empty for all)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function for_update(array $tables = []) : LockingClause
{
    return LockingClause::forUpdate(\array_values($tables));
}

/**
 * Create a FOR SHARE locking clause.
 *
 * @param list<string> $tables Tables to lock (empty for all)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function for_share(array $tables = []) : LockingClause
{
    return LockingClause::forShare(\array_values($tables));
}

/**
 * Create an ON CONFLICT DO NOTHING clause.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function on_conflict_nothing(?ConflictTarget $target = null) : OnConflictClause
{
    return OnConflictClause::doNothing($target);
}

/**
 * Create an ON CONFLICT DO UPDATE clause.
 *
 * @param ConflictTarget $target Conflict target (columns or constraint)
 * @param array<string, Expression> $updates Column updates
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function on_conflict_update(ConflictTarget $target, array $updates) : OnConflictClause
{
    return OnConflictClause::doUpdate($target, $updates);
}

/**
 * Create a conflict target for ON CONFLICT (columns).
 *
 * @param list<string> $columns Columns that define uniqueness
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function conflict_columns(array $columns) : ConflictTarget
{
    return ConflictTarget::columns(\array_values($columns));
}

/**
 * Create a conflict target for ON CONFLICT ON CONSTRAINT.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function conflict_constraint(string $name) : ConflictTarget
{
    return ConflictTarget::constraint($name);
}

/**
 * Create a RETURNING clause.
 *
 * @param Expression ...$expressions Expressions to return
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function returning(Expression ...$expressions) : ReturningClause
{
    return new ReturningClause(\array_values($expressions));
}

/**
 * Create a RETURNING * clause.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function returning_all() : ReturningClause
{
    return ReturningClause::all();
}

/**
 * Create a BEGIN transaction builder.
 *
 * Example: begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()
 * Produces: BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function begin() : BeginOptionsStep
{
    return BeginBuilder::create();
}

/**
 * Create a COMMIT transaction builder.
 *
 * Example: commit()->andChain()
 * Produces: COMMIT AND CHAIN
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function commit() : CommitOptionsStep
{
    return CommitBuilder::create();
}

/**
 * Create a ROLLBACK transaction builder.
 *
 * Example: rollback()->toSavepoint('my_savepoint')
 * Produces: ROLLBACK TO SAVEPOINT my_savepoint
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function rollback() : RollbackOptionsStep
{
    return RollbackBuilder::create();
}

/**
 * Create a SAVEPOINT.
 *
 * Example: savepoint('my_savepoint')
 * Produces: SAVEPOINT my_savepoint
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function savepoint(string $name) : SavepointFinalStep
{
    return SavepointBuilder::create($name);
}

/**
 * Release a SAVEPOINT.
 *
 * Example: release_savepoint('my_savepoint')
 * Produces: RELEASE my_savepoint
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function release_savepoint(string $name) : SavepointFinalStep
{
    return SavepointBuilder::release($name);
}

/**
 * Create a SET TRANSACTION builder.
 *
 * Example: set_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()
 * Produces: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function set_transaction() : SetTransactionOptionsStep
{
    return SetTransactionBuilder::create();
}

/**
 * Create a SET SESSION CHARACTERISTICS AS TRANSACTION builder.
 *
 * Example: set_session_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE)
 * Produces: SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function set_session_transaction() : SetTransactionOptionsStep
{
    return SetTransactionBuilder::session();
}

/**
 * Create a SET TRANSACTION SNAPSHOT builder.
 *
 * Example: transaction_snapshot('00000003-0000001A-1')
 * Produces: SET TRANSACTION SNAPSHOT '00000003-0000001A-1'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function transaction_snapshot(string $snapshotId) : SetTransactionFinalStep
{
    return SetTransactionBuilder::create()->snapshot($snapshotId);
}

/**
 * Create a PREPARE TRANSACTION builder.
 *
 * Example: prepare_transaction('my_transaction')
 * Produces: PREPARE TRANSACTION 'my_transaction'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function prepare_transaction(string $transactionId) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::prepare($transactionId);
}

/**
 * Create a COMMIT PREPARED builder.
 *
 * Example: commit_prepared('my_transaction')
 * Produces: COMMIT PREPARED 'my_transaction'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function commit_prepared(string $transactionId) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::commitPrepared($transactionId);
}

/**
 * Create a ROLLBACK PREPARED builder.
 *
 * Example: rollback_prepared('my_transaction')
 * Produces: ROLLBACK PREPARED 'my_transaction'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function rollback_prepared(string $transactionId) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::rollbackPrepared($transactionId);
}

/**
 * Declare a server-side cursor for a query.
 *
 * Cursors must be declared within a transaction and provide memory-efficient
 * iteration over large result sets via FETCH commands.
 *
 * Example with query builder:
 *   declare_cursor('my_cursor', select(star())->from(table('users')))->noScroll()
 *   Produces: DECLARE my_cursor NO SCROLL CURSOR FOR SELECT * FROM users
 *
 * Example with raw SQL:
 *   declare_cursor('my_cursor', 'SELECT * FROM users WHERE active = true')->withHold()
 *   Produces: DECLARE my_cursor NO SCROLL CURSOR WITH HOLD FOR SELECT * FROM users WHERE active = true
 *
 * @param string $cursorName Unique cursor name
 * @param SelectFinalStep|SqlQuery|string $query Query to iterate over
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function declare_cursor(string $cursorName, SelectFinalStep|string|SqlQuery $query) : DeclareCursorOptionsStep
{
    if ($query instanceof SelectFinalStep) {
        return DeclareCursorBuilder::create($cursorName, $query);
    }

    return DeclareCursorBuilder::createFromSql($cursorName, $query);
}

/**
 * Fetch rows from a cursor.
 *
 * Example: fetch('my_cursor')->forward(100)
 * Produces: FETCH FORWARD 100 my_cursor
 *
 * Example: fetch('my_cursor')->all()
 * Produces: FETCH ALL my_cursor
 *
 * @param string $cursorName Cursor to fetch from
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function fetch(string $cursorName) : FetchCursorBuilder
{
    return FetchCursorBuilder::create($cursorName);
}

/**
 * Close a cursor.
 *
 * Example: close_cursor('my_cursor')
 * Produces: CLOSE my_cursor
 *
 * Example: close_cursor() - closes all cursors
 * Produces: CLOSE ALL
 *
 * @param null|string $cursorName Cursor to close, or null to close all
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function close_cursor(?string $cursorName = null) : CloseCursorFinalStep
{
    if ($cursorName === null) {
        return CloseCursorBuilder::closeAll();
    }

    return CloseCursorBuilder::close($cursorName);
}

/**
 * Create a column definition for CREATE TABLE.
 *
 * @param string $name Column name
 * @param DataType $type Column data type
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function column(string $name, DataType $type) : ColumnDefinition
{
    return ColumnDefinition::create($name, $type);
}

/**
 * Create a PRIMARY KEY constraint.
 *
 * @param string ...$columns Columns that form the primary key
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function primary_key(string ...$columns) : PrimaryKeyConstraint
{
    return PrimaryKeyConstraint::create(...$columns);
}

/**
 * Create a UNIQUE constraint.
 *
 * @param string ...$columns Columns that must be unique together
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function unique_constraint(string ...$columns) : UniqueConstraint
{
    return UniqueConstraint::create(...$columns);
}

/**
 * Create a FOREIGN KEY constraint.
 *
 * @param list<string> $columns Local columns
 * @param string $referenceTable Referenced table
 * @param list<string> $referenceColumns Referenced columns (defaults to same as $columns if empty)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function foreign_key(array $columns, string $referenceTable, array $referenceColumns = []) : ForeignKeyConstraint
{
    return ForeignKeyConstraint::create($columns, $referenceTable, $referenceColumns);
}

/**
 * Create a CHECK constraint.
 *
 * @param string $expression SQL expression that must evaluate to true
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function check_constraint(string $expression) : CheckConstraint
{
    return CheckConstraint::create($expression);
}

/**
 * Create a factory for building CREATE statements.
 *
 * Provides a unified entry point for all CREATE operations:
 * - create()->table() - CREATE TABLE
 * - create()->tableAs() - CREATE TABLE AS
 * - create()->index() - CREATE INDEX
 * - create()->view() - CREATE VIEW
 * - create()->materializedView() - CREATE MATERIALIZED VIEW
 * - create()->sequence() - CREATE SEQUENCE
 * - create()->schema() - CREATE SCHEMA
 * - create()->role() - CREATE ROLE
 * - create()->function() - CREATE FUNCTION
 * - create()->procedure() - CREATE PROCEDURE
 * - create()->trigger() - CREATE TRIGGER
 * - create()->rule() - CREATE RULE
 * - create()->extension() - CREATE EXTENSION
 * - create()->compositeType() - CREATE TYPE (composite)
 * - create()->enumType() - CREATE TYPE (enum)
 * - create()->rangeType() - CREATE TYPE (range)
 * - create()->domain() - CREATE DOMAIN
 *
 * Example: create()->table('users')->columns(col_def('id', data_type_serial()))
 * Example: create()->index('idx_email')->on('users')->columns('email')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create() : CreateFactory
{
    return new CreateFactory();
}

/**
 * Create a factory for building DROP statements.
 *
 * Provides a unified entry point for all DROP operations:
 * - drop()->table() - DROP TABLE
 * - drop()->index() - DROP INDEX
 * - drop()->view() - DROP VIEW
 * - drop()->materializedView() - DROP MATERIALIZED VIEW
 * - drop()->sequence() - DROP SEQUENCE
 * - drop()->schema() - DROP SCHEMA
 * - drop()->role() - DROP ROLE
 * - drop()->function() - DROP FUNCTION
 * - drop()->procedure() - DROP PROCEDURE
 * - drop()->trigger() - DROP TRIGGER
 * - drop()->rule() - DROP RULE
 * - drop()->extension() - DROP EXTENSION
 * - drop()->type() - DROP TYPE
 * - drop()->domain() - DROP DOMAIN
 * - drop()->owned() - DROP OWNED
 *
 * Example: drop()->table('users', 'orders')->ifExists()->cascade()
 * Example: drop()->index('idx_email')->ifExists()
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop() : DropFactory
{
    return new DropFactory();
}

/**
 * Create a factory for building ALTER statements.
 *
 * Provides a unified entry point for all ALTER operations:
 * - alter()->table() - ALTER TABLE
 * - alter()->index() - ALTER INDEX
 * - alter()->view() - ALTER VIEW
 * - alter()->materializedView() - ALTER MATERIALIZED VIEW
 * - alter()->sequence() - ALTER SEQUENCE
 * - alter()->schema() - ALTER SCHEMA
 * - alter()->role() - ALTER ROLE
 * - alter()->function() - ALTER FUNCTION
 * - alter()->procedure() - ALTER PROCEDURE
 * - alter()->trigger() - ALTER TRIGGER
 * - alter()->extension() - ALTER EXTENSION
 * - alter()->enumType() - ALTER TYPE (enum)
 * - alter()->domain() - ALTER DOMAIN
 *
 * Rename operations are also under alter():
 * - alter()->index('old')->renameTo('new')
 * - alter()->view('old')->renameTo('new')
 * - alter()->schema('old')->renameTo('new')
 * - alter()->role('old')->renameTo('new')
 * - alter()->trigger('old')->on('table')->renameTo('new')
 *
 * Example: alter()->table('users')->addColumn(col_def('email', data_type_text()))
 * Example: alter()->sequence('user_id_seq')->restart(1000)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter() : AlterFactory
{
    return new AlterFactory();
}

/**
 * Create a TRUNCATE TABLE builder.
 *
 * @param string ...$tables Table names to truncate
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function truncate_table(string ...$tables) : TruncateFinalStep
{
    return TruncateBuilder::create(...$tables);
}

/**
 * Create a REFRESH MATERIALIZED VIEW builder.
 *
 * Example: refresh_materialized_view('user_stats')
 * Produces: REFRESH MATERIALIZED VIEW user_stats
 *
 * Example: refresh_materialized_view('user_stats')->concurrently()->withData()
 * Produces: REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA
 *
 * @param string $name View name (may include schema as "schema.view")
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function refresh_materialized_view(string $name, ?string $schema = null) : RefreshMatViewOptionsStep
{
    return RefreshMaterializedViewBuilder::create($name, $schema);
}

/**
 * Get a CASCADE referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_cascade() : ReferentialAction
{
    return ReferentialAction::CASCADE;
}

/**
 * Get a RESTRICT referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_restrict() : ReferentialAction
{
    return ReferentialAction::RESTRICT;
}

/**
 * Get a SET NULL referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_set_null() : ReferentialAction
{
    return ReferentialAction::SET_NULL;
}

/**
 * Get a SET DEFAULT referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_set_default() : ReferentialAction
{
    return ReferentialAction::SET_DEFAULT;
}

/**
 * Get a NO ACTION referential action.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ref_action_no_action() : ReferentialAction
{
    return ReferentialAction::NO_ACTION;
}

/**
 * Start building a REINDEX INDEX statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_index('idx_users_email')->concurrently()
 *
 * @param string $name The index name (may include schema: schema.index)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_index(string $name) : ReindexFinalStep
{
    return ReindexBuilder::index($name);
}

/**
 * Start building a REINDEX TABLE statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_table('users')->concurrently()
 *
 * @param string $name The table name (may include schema: schema.table)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_table(string $name) : ReindexFinalStep
{
    return ReindexBuilder::table($name);
}

/**
 * Start building a REINDEX SCHEMA statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_schema('public')->concurrently()
 *
 * @param string $name The schema name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_schema(string $name) : ReindexFinalStep
{
    return ReindexBuilder::schema($name);
}

/**
 * Start building a REINDEX DATABASE statement.
 *
 * Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()
 *
 * Example: reindex_database('mydb')->concurrently()
 *
 * @param string $name The database name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function reindex_database(string $name) : ReindexFinalStep
{
    return ReindexBuilder::database($name);
}

/**
 * Create an index column specification.
 *
 * Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()
 *
 * Example: index_col('email')->desc()->nullsLast()
 *
 * @param string $name The column name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function index_col(string $name) : IndexColumn
{
    return IndexColumn::column($name);
}

/**
 * Create an index column specification from an expression.
 *
 * Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()
 *
 * Example: index_expr(fn_call('lower', col('email')))->desc()
 *
 * @param Expression $expression The expression to index
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function index_expr(Expression $expression) : IndexColumn
{
    return IndexColumn::expression($expression);
}

/**
 * Get the BTREE index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_btree() : IndexMethod
{
    return IndexMethod::BTREE;
}

/**
 * Get the HASH index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_hash() : IndexMethod
{
    return IndexMethod::HASH;
}

/**
 * Get the GIST index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_gist() : IndexMethod
{
    return IndexMethod::GIST;
}

/**
 * Get the SPGIST index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_spgist() : IndexMethod
{
    return IndexMethod::SPGIST;
}

/**
 * Get the GIN index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_gin() : IndexMethod
{
    return IndexMethod::GIN;
}

/**
 * Get the BRIN index method.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function index_method_brin() : IndexMethod
{
    return IndexMethod::BRIN;
}

/**
 * Create a VACUUM builder.
 *
 * Example: vacuum()->table('users')
 * Produces: VACUUM users
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function vacuum() : VacuumFinalStep
{
    return VacuumBuilder::create();
}

/**
 * Create an ANALYZE builder.
 *
 * Example: analyze()->table('users')
 * Produces: ANALYZE users
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function analyze() : AnalyzeFinalStep
{
    return AnalyzeBuilder::create();
}

/**
 * Create an EXPLAIN builder for a query.
 *
 * Example: explain(select()->from('users'))
 * Produces: EXPLAIN SELECT * FROM users
 *
 * @param DeleteBuilder|InsertBuilder|SelectFinalStep|UpdateBuilder $query Query to explain
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function explain(SelectFinalStep|InsertBuilder|UpdateBuilder|DeleteBuilder $query) : ExplainFinalStep
{
    return ExplainBuilder::create($query);
}

/**
 * Create a LOCK TABLE builder.
 *
 * Example: lock_table('users', 'orders')->accessExclusive()
 * Produces: LOCK TABLE users, orders IN ACCESS EXCLUSIVE MODE
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lock_table(string ...$tables) : LockFinalStep
{
    return LockBuilder::create(...$tables);
}

/**
 * Create a COMMENT ON builder.
 *
 * Example: comment(CommentTarget::TABLE, 'users')->is('User accounts table')
 * Produces: COMMENT ON TABLE users IS 'User accounts table'
 *
 * @param CommentTarget $target Target type (TABLE, COLUMN, INDEX, etc.)
 * @param string $name Target name (use 'table.column' for COLUMN targets)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function comment(CommentTarget $target, string $name) : CommentFinalStep
{
    return CommentBuilder::create($target, $name);
}

/**
 * Create a CLUSTER builder.
 *
 * Example: cluster()->table('users')->using('idx_users_pkey')
 * Produces: CLUSTER users USING idx_users_pkey
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cluster() : ClusterFinalStep
{
    return ClusterBuilder::create();
}

/**
 * Create a DISCARD builder.
 *
 * Example: discard(DiscardType::ALL)
 * Produces: DISCARD ALL
 *
 * @param DiscardType $type Type of resources to discard (ALL, PLANS, SEQUENCES, TEMP)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function discard(DiscardType $type) : DiscardFinalStep
{
    return DiscardBuilder::create($type);
}

/**
 * Create a GRANT privileges builder.
 *
 * Example: grant(TablePrivilege::SELECT)->onTable('users')->to('app_user')
 * Produces: GRANT SELECT ON users TO app_user
 *
 * Example: grant(TablePrivilege::ALL)->onAllTablesInSchema('public')->to('admin')
 * Produces: GRANT ALL ON ALL TABLES IN SCHEMA public TO admin
 *
 * @param string|TablePrivilege ...$privileges The privileges to grant
 *
 * @return GrantOnStep Builder for grant options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function grant(TablePrivilege|string ...$privileges) : GrantOnStep
{
    return GrantBuilder::create(...$privileges);
}

/**
 * Create a GRANT role builder.
 *
 * Example: grant_role('admin')->to('user1')
 * Produces: GRANT admin TO user1
 *
 * Example: grant_role('admin', 'developer')->to('user1')->withAdminOption()
 * Produces: GRANT admin, developer TO user1 WITH ADMIN OPTION
 *
 * @param string ...$roles The roles to grant
 *
 * @return GrantRoleToStep Builder for grant role options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function grant_role(string ...$roles) : GrantRoleToStep
{
    return GrantRoleBuilder::create(...$roles);
}

/**
 * Create a REVOKE privileges builder.
 *
 * Example: revoke(TablePrivilege::SELECT)->onTable('users')->from('app_user')
 * Produces: REVOKE SELECT ON users FROM app_user
 *
 * Example: revoke(TablePrivilege::ALL)->onTable('users')->from('app_user')->cascade()
 * Produces: REVOKE ALL ON users FROM app_user CASCADE
 *
 * @param string|TablePrivilege ...$privileges The privileges to revoke
 *
 * @return RevokeOnStep Builder for revoke options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function revoke(TablePrivilege|string ...$privileges) : RevokeOnStep
{
    return RevokeBuilder::create(...$privileges);
}

/**
 * Create a REVOKE role builder.
 *
 * Example: revoke_role('admin')->from('user1')
 * Produces: REVOKE admin FROM user1
 *
 * Example: revoke_role('admin')->from('user1')->cascade()
 * Produces: REVOKE admin FROM user1 CASCADE
 *
 * @param string ...$roles The roles to revoke
 *
 * @return RevokeRoleFromStep Builder for revoke role options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function revoke_role(string ...$roles) : RevokeRoleFromStep
{
    return RevokeRoleBuilder::create(...$roles);
}

/**
 * Create a SET ROLE builder.
 *
 * Example: set_role('admin')
 * Produces: SET ROLE admin
 *
 * @param string $role The role to set
 *
 * @return SetRoleFinalStep Builder for set role
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function set_role(string $role) : SetRoleFinalStep
{
    return SetRoleBuilder::create($role);
}

/**
 * Create a RESET ROLE builder.
 *
 * Example: reset_role()
 * Produces: RESET ROLE
 *
 * @return ResetRoleFinalStep Builder for reset role
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function reset_role() : ResetRoleFinalStep
{
    return ResetRoleBuilder::create();
}

/**
 * Create a REASSIGN OWNED builder.
 *
 * Example: reassign_owned('old_role')->to('new_role')
 * Produces: REASSIGN OWNED BY old_role TO new_role
 *
 * @param string ...$roles The roles whose owned objects should be reassigned
 *
 * @return ReassignOwnedToStep Builder for reassign owned options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function reassign_owned(string ...$roles) : ReassignOwnedToStep
{
    return ReassignOwnedBuilder::create(...$roles);
}

/**
 * Create a DROP OWNED builder.
 *
 * Example: drop_owned('role1')
 * Produces: DROP OWNED BY role1
 *
 * Example: drop_owned('role1', 'role2')->cascade()
 * Produces: DROP OWNED BY role1, role2 CASCADE
 *
 * @param string ...$roles The roles whose owned objects should be dropped
 *
 * @return DropOwnedFinalStep Builder for drop owned options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_owned(string ...$roles) : DropOwnedFinalStep
{
    return DropOwnedBuilder::create(...$roles);
}

/**
 * Creates a new function argument for use in function/procedure definitions.
 *
 * Example: func_arg(data_type_integer())
 * Example: func_arg(data_type_text())->named('username')
 * Example: func_arg(data_type_integer())->named('count')->default('0')
 * Example: func_arg(data_type_text())->out()
 *
 * @param DataType $type The PostgreSQL data type for the argument
 *
 * @return FunctionArgument Builder for function argument options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function func_arg(DataType $type) : FunctionArgument
{
    return FunctionArgument::of($type);
}

/**
 * Creates a CALL statement builder for invoking a procedure.
 *
 * Example: call('update_stats')->with(123)
 * Produces: CALL update_stats(123)
 *
 * Example: call('process_data')->with('test', 42, true)
 * Produces: CALL process_data('test', 42, true)
 *
 * @param string $procedure The name of the procedure to call
 *
 * @return CallFinalStep Builder for call statement options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function call(string $procedure) : CallFinalStep
{
    return CallBuilder::create($procedure);
}

/**
 * Creates a DO statement builder for executing an anonymous code block.
 *
 * Example: do_block('BEGIN RAISE NOTICE $$Hello World$$; END;')
 * Produces: DO $$ BEGIN RAISE NOTICE $$Hello World$$; END; $$ LANGUAGE plpgsql
 *
 * Example: do_block('SELECT 1')->language('sql')
 * Produces: DO $$ SELECT 1 $$ LANGUAGE sql
 *
 * @param string $code The anonymous code block to execute
 *
 * @return DoFinalStep Builder for DO statement options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function do_block(string $code) : DoFinalStep
{
    return DoBuilder::create($code);
}

/**
 * Creates a type attribute for composite types.
 *
 * Example: type_attr('name', data_type_text())
 * Produces: name text
 *
 * Example: type_attr('description', data_type_text())->collate('en_US')
 * Produces: description text COLLATE "en_US"
 *
 * @param string $name The attribute name
 * @param DataType $type The attribute type
 *
 * @return TypeAttribute Type attribute value object
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function type_attr(string $name, DataType $type) : TypeAttribute
{
    return TypeAttribute::of($name, $type);
}

/**
 * Create connection parameters from a connection string.
 *
 * Accepts libpq-style connection strings:
 * - Key-value format: "host=localhost port=5432 dbname=mydb user=myuser password=secret"
 * - URI format: "postgresql://user:password@localhost:5432/dbname"
 *
 * @example
 * $params = pgsql_connection('host=localhost dbname=mydb');
 * $params = pgsql_connection('postgresql://user:pass@localhost/mydb');
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection(#[\SensitiveParameter] string $connectionString) : ConnectionParameters
{
    return ConnectionParameters::fromString($connectionString);
}

/**
 * Create connection parameters from a DSN string.
 *
 * Parses standard PostgreSQL DSN format commonly used in environment variables
 * (e.g., DATABASE_URL). Supports postgres://, postgresql://, and pgsql:// schemes.
 *
 * @param string $dsn DSN string in format: postgres://user:password@host:port/database?options
 *
 * @throws Client\DsnParserException If the DSN cannot be parsed
 *
 * @example
 * $params = pgsql_connection_dsn('postgres://myuser:secret@localhost:5432/mydb');
 * $params = pgsql_connection_dsn('postgresql://user:pass@db.example.com/app?sslmode=require');
 * $params = pgsql_connection_dsn('pgsql://user:pass@localhost/mydb'); // Symfony/Doctrine format
 * $params = pgsql_connection_dsn(getenv('DATABASE_URL'));
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection_dsn(#[\SensitiveParameter] string $dsn) : ConnectionParameters
{
    return (new DsnParser())->parse($dsn);
}

/**
 * Create connection parameters from individual values.
 *
 * Allows specifying connection parameters individually for better type safety
 * and IDE support.
 *
 * @param string $database Database name (required)
 * @param string $host Hostname (default: localhost)
 * @param int $port Port number (default: 5432)
 * @param null|string $user Username (optional)
 * @param null|string $password Password (optional)
 * @param array<string, string> $options Additional libpq options
 *
 * @example
 * $params = pgsql_connection_params(
 *     database: 'mydb',
 *     host: 'localhost',
 *     user: 'myuser',
 *     password: 'secret',
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection_params(
    string $database,
    string $host = 'localhost',
    int $port = 5432,
    ?string $user = null,
    ?string $password = null,
    array $options = [],
) : ConnectionParameters {
    return ConnectionParameters::fromParams(
        database: $database,
        host: $host,
        port: $port,
        user: $user,
        password: $password,
        options: $options,
    );
}

/**
 * Create a PostgreSQL client using ext-pgsql.
 *
 * The client connects immediately and is ready to execute queries.
 * For object mapping, provide a RowMapper (use pgsql_mapper() for the default).
 *
 * @param Client\ConnectionParameters $params Connection parameters
 * @param null|ValueConverters $valueConverters Custom type converters (optional)
 * @param null|Client\RowMapper $mapper Row mapper for object hydration (optional)
 *
 * @throws ConnectionException If connection fails
 *
 * @example
 * // Basic client
 * $client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));
 *
 * // With object mapping
 * $client = pgsql_client(
 *     pgsql_connection('host=localhost dbname=mydb'),
 *     mapper: pgsql_mapper(),
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_client(
    ConnectionParameters $params,
    ?ValueConverters $valueConverters = null,
    ?RowMapper $mapper = null,
) : Client\Client {
    return PgSqlClient::connect($params, $valueConverters, $mapper);
}

/**
 * Create a default constructor-based row mapper.
 *
 * Maps database rows directly to constructor parameters.
 * Column names must match parameter names exactly (1:1).
 * Use SQL aliases if column names differ from parameter names.
 *
 * @example
 * // DTO where column names match parameter names
 * readonly class User {
 *     public function __construct(
 *         public int $id,
 *         public string $name,
 *         public string $email,
 *     ) {}
 * }
 *
 * // Usage
 * $client = pgsql_client(pgsql_connection('...'), mapper: pgsql_mapper());
 *
 * // For snake_case columns, use SQL aliases
 * $user = $client->fetchInto(
 *     User::class,
 *     'SELECT id, user_name AS name, user_email AS email FROM users WHERE id = $1',
 *     [1]
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_mapper() : ConstructorMapper
{
    return new ConstructorMapper();
}

/**
 * Wrap a value with explicit PostgreSQL type information for parameter binding.
 *
 * Use when auto-detection isn't sufficient or when you need to specify
 * the exact PostgreSQL type (since one PHP type can map to multiple PostgreSQL types):
 * - int could be INT2, INT4, or INT8
 * - string could be TEXT, VARCHAR, or CHAR
 * - array must always use typed() since auto-detection cannot determine element type
 * - DateTimeInterface could be TIMESTAMP or TIMESTAMPTZ
 * - Json could be JSON or JSONB
 *
 * @param mixed $value The value to bind
 * @param PostgreSqlType $targetType The PostgreSQL type to convert the value to
 *
 * @example
 * $client->fetch(
 *     'SELECT * FROM users WHERE id = $1 AND tags = $2',
 *     [
 *         typed('550e8400-e29b-41d4-a716-446655440000', PostgreSqlType::UUID),
 *         typed(['tag1', 'tag2'], PostgreSqlType::TEXT_ARRAY),
 *     ]
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function typed(mixed $value, PostgreSqlType $targetType) : TypedValue
{
    return new TypedValue($value, $targetType);
}

// PostgreSqlType DSL functions - Scalar types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_text() : PostgreSqlType
{
    return PostgreSqlType::TEXT;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_varchar() : PostgreSqlType
{
    return PostgreSqlType::VARCHAR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_char() : PostgreSqlType
{
    return PostgreSqlType::CHAR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bpchar() : PostgreSqlType
{
    return PostgreSqlType::BPCHAR;
}

// PostgreSqlType DSL functions - Integer types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int2() : PostgreSqlType
{
    return PostgreSqlType::INT2;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_smallint() : PostgreSqlType
{
    return PostgreSqlType::INT2;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int4() : PostgreSqlType
{
    return PostgreSqlType::INT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_integer() : PostgreSqlType
{
    return PostgreSqlType::INT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int8() : PostgreSqlType
{
    return PostgreSqlType::INT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bigint() : PostgreSqlType
{
    return PostgreSqlType::INT8;
}

// PostgreSqlType DSL functions - Floating point types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_float4() : PostgreSqlType
{
    return PostgreSqlType::FLOAT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_real() : PostgreSqlType
{
    return PostgreSqlType::FLOAT4;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_float8() : PostgreSqlType
{
    return PostgreSqlType::FLOAT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_double() : PostgreSqlType
{
    return PostgreSqlType::FLOAT8;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_numeric() : PostgreSqlType
{
    return PostgreSqlType::NUMERIC;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_money() : PostgreSqlType
{
    return PostgreSqlType::MONEY;
}

// PostgreSqlType DSL functions - Boolean type

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bool() : PostgreSqlType
{
    return PostgreSqlType::BOOL;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_boolean() : PostgreSqlType
{
    return PostgreSqlType::BOOL;
}

// PostgreSqlType DSL functions - Binary types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bytea() : PostgreSqlType
{
    return PostgreSqlType::BYTEA;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bit() : PostgreSqlType
{
    return PostgreSqlType::BIT;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_varbit() : PostgreSqlType
{
    return PostgreSqlType::VARBIT;
}

// PostgreSqlType DSL functions - Date/time types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_date() : PostgreSqlType
{
    return PostgreSqlType::DATE;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_time() : PostgreSqlType
{
    return PostgreSqlType::TIME;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_timetz() : PostgreSqlType
{
    return PostgreSqlType::TIMETZ;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_timestamp() : PostgreSqlType
{
    return PostgreSqlType::TIMESTAMP;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_timestamptz() : PostgreSqlType
{
    return PostgreSqlType::TIMESTAMPTZ;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_interval() : PostgreSqlType
{
    return PostgreSqlType::INTERVAL;
}

// PostgreSqlType DSL functions - JSON types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_json() : PostgreSqlType
{
    return PostgreSqlType::JSON;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_jsonb() : PostgreSqlType
{
    return PostgreSqlType::JSONB;
}

// PostgreSqlType DSL functions - UUID type

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_uuid() : PostgreSqlType
{
    return PostgreSqlType::UUID;
}

// PostgreSqlType DSL functions - Network types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_inet() : PostgreSqlType
{
    return PostgreSqlType::INET;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_cidr() : PostgreSqlType
{
    return PostgreSqlType::CIDR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_macaddr() : PostgreSqlType
{
    return PostgreSqlType::MACADDR;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_macaddr8() : PostgreSqlType
{
    return PostgreSqlType::MACADDR8;
}

// PostgreSqlType DSL functions - Other types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_xml() : PostgreSqlType
{
    return PostgreSqlType::XML;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_oid() : PostgreSqlType
{
    return PostgreSqlType::OID;
}

// PostgreSqlType DSL functions - Array types

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_text_array() : PostgreSqlType
{
    return PostgreSqlType::TEXT_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_varchar_array() : PostgreSqlType
{
    return PostgreSqlType::VARCHAR_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int2_array() : PostgreSqlType
{
    return PostgreSqlType::INT2_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int4_array() : PostgreSqlType
{
    return PostgreSqlType::INT4_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_int8_array() : PostgreSqlType
{
    return PostgreSqlType::INT8_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_float4_array() : PostgreSqlType
{
    return PostgreSqlType::FLOAT4_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_float8_array() : PostgreSqlType
{
    return PostgreSqlType::FLOAT8_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_bool_array() : PostgreSqlType
{
    return PostgreSqlType::BOOL_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_uuid_array() : PostgreSqlType
{
    return PostgreSqlType::UUID_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_json_array() : PostgreSqlType
{
    return PostgreSqlType::JSON_ARRAY;
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_type_jsonb_array() : PostgreSqlType
{
    return PostgreSqlType::JSONB_ARRAY;
}
