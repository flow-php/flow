<?php

declare(strict_types=1);

namespace Flow\PgQuery\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\PgQuery\AST\Transformers\{CountModifier, KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, PaginationConfig, PaginationModifier, SortOrder};
use Flow\PgQuery\{DeparseOptions, ParsedQuery, Parser};
use Flow\PgQuery\Extractors\{Columns, Functions, Tables};
use Flow\PgQuery\Protobuf\AST\Node;
use Flow\PgQuery\QueryBuilder\Clause\{
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
    OrderByItem,
    ReturningClause,
    SortDirection,
    WindowDefinition,
    WindowFrame
};
use Flow\PgQuery\QueryBuilder\Clause\{OrderBy, WithClause};
use Flow\PgQuery\QueryBuilder\Condition\{
    All,
    AndCondition,
    Any,
    Between,
    Comparison,
    ComparisonOperator,
    Condition,
    Exists,
    In,
    IsDistinctFrom,
    IsNull,
    Like,
    NotCondition,
    OrCondition,
    RawCondition,
    SimilarTo
};
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFromStep};
use Flow\PgQuery\QueryBuilder\Expression\{
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
    Star,
    Subquery,
    TypeCast,
    WhenClause,
    WindowFunction
};
use Flow\PgQuery\QueryBuilder\Insert\{InsertBuilder, InsertIntoStep};
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFinalStep, SelectSelectStep};
use Flow\PgQuery\QueryBuilder\Table\{
    CTEReference,
    DerivedTable,
    Lateral,
    Table,
    TableFunction,
    TableReference
};
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateTableStep};

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
 * Create a new SELECT query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function select() : SelectSelectStep
{
    return SelectBuilder::create();
}

/**
 * Create a SELECT query builder with a WITH clause (CTE).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function select_with(WithClause $with) : SelectSelectStep
{
    return SelectBuilder::with($with);
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
 * Parse SQL and convert to a QueryBuilder for modification.
 *
 * Only works for single-statement queries. For multiple statements,
 * use pg_split() to parse statements individually.
 *
 * @throws \InvalidArgumentException if query contains multiple statements or unsupported statement type
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_query_builder(string $sql) : SelectBuilder|InsertBuilder|UpdateBuilder|DeleteBuilder
{
    return sql_parse($sql)->toQueryBuilder();
}

// ----------------------------------------------------------------------------
// Expressions
// ----------------------------------------------------------------------------

/**
 * Create a column reference expression.
 *
 * @param string $name Column name (can include table prefix like "users.id" or "schema.table.column")
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function col_from_string(string $name) : Column
{
    $parts = \explode('.', $name);

    return Column::fromParts($parts);
}

/**
 * Create a column reference with explicit table/schema parts.
 *
 * @param string $column Column name
 * @param null|string $table Table name (optional)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function col(string $column, ?string $table = null, ?string $schema = null) : Column
{
    if ($schema !== null && $table !== null) {
        return Column::schemaTableColumn($schema, $table, $column);
    }

    if ($table !== null) {
        return Column::tableColumn($table, $column);
    }

    return Column::name($column);
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
 * Create a string literal.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal_string(string $value) : Literal
{
    return Literal::string($value);
}

/**
 * Create an integer literal.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal_int(int $value) : Literal
{
    return Literal::int($value);
}

/**
 * Create a float literal.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal_float(float $value) : Literal
{
    return Literal::float($value);
}

/**
 * Create a boolean literal.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal_bool(bool $value) : Literal
{
    return Literal::bool($value);
}

/**
 * Create a NULL literal.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function literal_null() : Literal
{
    return Literal::null();
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
    $nameParts = \explode('.', $name);

    return new FunctionCall($nameParts, $args);
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
 * @param string $type Target type name (can include schema like "pg_catalog.int4")
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cast(Expression $expr, string $type) : TypeCast
{
    $typeParts = \explode('.', $type);

    return new TypeCast($expr, $typeParts);
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
 * @param list<OrderBy|OrderByItem> $orderBy ORDER BY items
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

// ----------------------------------------------------------------------------
// Conditions
// ----------------------------------------------------------------------------

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
function all_sub_selects(Expression $left, ComparisonOperator $operator, SelectFinalStep $subquery) : All
{
    $node = new Node();
    $node->setSelectStmt($subquery->toAst());

    return new All($left, $operator, $node);
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
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function raw_cond(string $sql) : RawCondition
{
    return new RawCondition($sql);
}

// ----------------------------------------------------------------------------
// Table References
// ----------------------------------------------------------------------------

/**
 * Create a table reference.
 *
 * @param string $name Table name
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function table(string $name, ?string $schema = null) : Table
{
    return new Table($name, $schema);
}

/**
 * Create a CTE (Common Table Expression) reference.
 *
 * @param non-empty-string $name CTE name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cte_ref(string $name) : CTEReference
{
    if ($name === '') {
        throw new \InvalidArgumentException('CTE name cannot be empty');
    }

    return new CTEReference($name);
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

// ----------------------------------------------------------------------------
// Clauses
// ----------------------------------------------------------------------------

/**
 * Create an ORDER BY item.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function order_by(
    Expression $expr,
    SortDirection $direction = SortDirection::ASC,
    NullsPosition $nulls = NullsPosition::DEFAULT,
) : OrderByItem {
    return new OrderByItem($expr, $direction, $nulls);
}

/**
 * Create an ORDER BY item with ASC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function asc(Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT) : OrderByItem
{
    return new OrderByItem($expr, SortDirection::ASC, $nulls);
}

/**
 * Create an ORDER BY item with DESC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function desc(Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT) : OrderByItem
{
    return new OrderByItem($expr, SortDirection::DESC, $nulls);
}

/**
 * Create a WITH clause (CTE container).
 *
 * @param array<CTE> $ctes CTEs to include
 * @param bool $recursive Whether this is a recursive WITH
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function with_cte(array $ctes, bool $recursive = false) : WithClause
{
    return new WithClause($ctes, $recursive);
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
 * @param list<OrderBy|OrderByItem> $orderBy ORDER BY items
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
