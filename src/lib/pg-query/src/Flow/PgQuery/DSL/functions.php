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
    OperatorCondition,
    OrCondition,
    RawCondition,
    SimilarTo
};
use Flow\PgQuery\QueryBuilder\Copy\{CopyFromBuilder, CopyFromTableStep, CopyToBuilder, CopyToTableStep};
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFromStep};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
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
use Flow\PgQuery\QueryBuilder\Merge\{MergeBuilder, MergeUsingStep};
use Flow\PgQuery\QueryBuilder\QualifiedIdentifier;
use Flow\PgQuery\QueryBuilder\Schema\AlterSequence\{AlterSequenceBuilder, AlterSequenceOptionsStep};
use Flow\PgQuery\QueryBuilder\Schema\AlterTable\{AlterTableBuilder, AlterTableFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\{ColumnDefinition, DataType, ReferentialAction};
use Flow\PgQuery\QueryBuilder\Schema\Constraint\{CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint};
use Flow\PgQuery\QueryBuilder\Schema\CreateSequence\{CreateSequenceBuilder, CreateSequenceOptionsStep};
use Flow\PgQuery\QueryBuilder\Schema\CreateTable\{CreateTableBuilder, CreateTableColumnsStep};
use Flow\PgQuery\QueryBuilder\Schema\CreateTableAs\{CreateTableAsBuilder, CreateTableAsFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Domain\{
    AlterDomainActionStep,
    AlterDomainBuilder,
    CreateDomainBuilder,
    CreateDomainTypeStep,
    DropDomainBuilder,
    DropDomainFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\DropSequence\{DropSequenceBuilder, DropSequenceFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\DropTable\{DropTableBuilder, DropTableFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Extension\{
    AlterExtensionActionStep,
    AlterExtensionBuilder,
    CreateExtensionBuilder,
    CreateExtensionOptionsStep,
    DropExtensionBuilder,
    DropExtensionFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\Function\{
    AlterFunctionArgsStep,
    AlterFunctionBuilder,
    AlterProcedureArgsStep,
    AlterProcedureBuilder,
    CallBuilder,
    CallFinalStep,
    CreateFunctionArgsStep,
    CreateFunctionBuilder,
    CreateProcedureArgsStep,
    CreateProcedureBuilder,
    DoBuilder,
    DoFinalStep,
    DropFunctionBuilder,
    DropFunctionFinalStep,
    DropProcedureBuilder,
    DropProcedureFinalStep,
    FunctionArgument
};
use Flow\PgQuery\QueryBuilder\Schema\Grant\{
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
use Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex\{AlterIndexBuilder, AlterIndexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex\{CreateIndexBuilder, CreateIndexOnStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\DropIndex\{DropIndexBuilder, DropIndexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\{IndexColumn, IndexMethod};
use Flow\PgQuery\QueryBuilder\Schema\Index\Reindex\{ReindexBuilder, ReindexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Ownership\{
    DropOwnedBuilder,
    DropOwnedFinalStep,
    ReassignOwnedBuilder,
    ReassignOwnedToStep
};
use Flow\PgQuery\QueryBuilder\Schema\Role\{
    AlterRoleActionStep,
    AlterRoleBuilder,
    CreateRoleBuilder,
    CreateRoleOptionsStep,
    DropRoleBuilder,
    DropRoleFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\Rule\{
    CreateRuleBuilder,
    CreateRuleEventStep,
    DropRuleBuilder,
    DropRuleOnStep
};
use Flow\PgQuery\QueryBuilder\Schema\Schema\{
    AlterSchemaActionStep,
    AlterSchemaBuilder,
    CreateSchemaBuilder,
    CreateSchemaOptionsStep,
    DropSchemaBuilder,
    DropSchemaFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\Session\{
    ResetRoleBuilder,
    ResetRoleFinalStep,
    SetRoleBuilder,
    SetRoleFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\Trigger\{
    AlterTriggerBuilder,
    AlterTriggerOnStep,
    CreateTriggerBuilder,
    CreateTriggerTimingStep,
    DropTriggerBuilder,
    DropTriggerOnStep
};
use Flow\PgQuery\QueryBuilder\Schema\Truncate\{TruncateBuilder, TruncateFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Type\{
    AlterEnumTypeActionStep,
    AlterEnumTypeBuilder,
    CreateCompositeTypeAttributesStep,
    CreateCompositeTypeBuilder,
    CreateEnumTypeBuilder,
    CreateEnumTypeLabelsStep,
    CreateRangeTypeBuilder,
    CreateRangeTypeSubtypeStep,
    DropTypeBuilder,
    DropTypeFinalStep,
    TypeAttribute
};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView\{AlterMatViewActionStep, AlterMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterView\{AlterViewActionStep, AlterViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView\{CreateMatViewOptionsStep, CreateMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\CreateView\{CreateViewBuilder, CreateViewOptionsStep};
use Flow\PgQuery\QueryBuilder\Schema\View\DropMaterializedView\{DropMatViewFinalStep, DropMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Schema\View\DropView\{DropViewBuilder, DropViewFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\View\RefreshMaterializedView\{RefreshMatViewOptionsStep, RefreshMaterializedViewBuilder};
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFinalStep, SelectSelectStep};
use Flow\PgQuery\QueryBuilder\Table\{
    CTEReference,
    DerivedTable,
    Lateral,
    Table,
    TableFunction,
    TableReference
};
use Flow\PgQuery\QueryBuilder\Transaction\{
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
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateTableStep};
use Flow\PgQuery\QueryBuilder\Utility\{
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
    LockBuilder,
    LockFinalStep,
    VacuumBuilder,
    VacuumFinalStep
};
use Flow\PgQuery\QueryBuilder\With\WithBuilder;

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
 * Example: with(with_cte([cte('users', $subquery)]))->select(star())->from(cte_ref('users'))
 * Example: with(with_cte([cte('data', $subquery)], recursive: true))->select(col('id'))->from(...)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function with(WithClause $clause) : WithBuilder
{
    return new WithBuilder($clause);
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
 * Create a new COPY TO query builder for data export.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function copy_to() : CopyToTableStep
{
    return CopyToBuilder::create();
}

/**
 * Create a new COPY FROM query builder for data import.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function copy_from() : CopyFromTableStep
{
    return CopyFromBuilder::create();
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
 * @param string $type Target type name (can include schema like "pg_catalog.int4")
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cast(Expression $expr, string $type) : TypeCast
{
    return new TypeCast($expr, QualifiedIdentifier::parse($type)->parts());
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
// JSONB Operators
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Array Operators
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Pattern Matching (Regex) Operators
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Full-Text Search Operators
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Table References
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Transaction Commands
// ----------------------------------------------------------------------------

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
function prepare_transaction(string $gid) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::prepare($gid);
}

/**
 * Create a COMMIT PREPARED builder.
 *
 * Example: commit_prepared('my_transaction')
 * Produces: COMMIT PREPARED 'my_transaction'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function commit_prepared(string $gid) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::commitPrepared($gid);
}

/**
 * Create a ROLLBACK PREPARED builder.
 *
 * Example: rollback_prepared('my_transaction')
 * Produces: ROLLBACK PREPARED 'my_transaction'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function rollback_prepared(string $gid) : PreparedTransactionFinalStep
{
    return PreparedTransactionBuilder::rollbackPrepared($gid);
}

// ----------------------------------------------------------------------------
// SQL Data Types
// ----------------------------------------------------------------------------

/**
 * Create an INTEGER data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_integer() : DataType
{
    return DataType::integer();
}

/**
 * Create a BIGINT data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_bigint() : DataType
{
    return DataType::bigint();
}

/**
 * Create a SMALLINT data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_smallint() : DataType
{
    return DataType::smallint();
}

/**
 * Create a SERIAL (auto-incrementing integer) data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_serial() : DataType
{
    return DataType::serial();
}

/**
 * Create a BIGSERIAL (auto-incrementing bigint) data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_bigserial() : DataType
{
    return DataType::bigserial();
}

/**
 * Create a SMALLSERIAL (auto-incrementing smallint) data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_smallserial() : DataType
{
    return DataType::smallserial();
}

/**
 * Create a TEXT data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_text() : DataType
{
    return DataType::text();
}

/**
 * Create a VARCHAR data type.
 *
 * @param int $length Maximum character length
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_varchar(int $length) : DataType
{
    return DataType::varchar($length);
}

/**
 * Create a CHAR data type.
 *
 * @param int $length Fixed character length
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_char(int $length) : DataType
{
    return DataType::char($length);
}

/**
 * Create a BOOLEAN data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_boolean() : DataType
{
    return DataType::boolean();
}

/**
 * Create a DATE data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_date() : DataType
{
    return DataType::date();
}

/**
 * Create a TIME data type.
 *
 * @param null|int $precision Fractional seconds precision
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_time(?int $precision = null) : DataType
{
    return DataType::time($precision);
}

/**
 * Create a TIMESTAMP data type.
 *
 * @param null|int $precision Fractional seconds precision
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_timestamp(?int $precision = null) : DataType
{
    return DataType::timestamp($precision);
}

/**
 * Create a TIMESTAMP WITH TIME ZONE data type.
 *
 * @param null|int $precision Fractional seconds precision
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_timestamptz(?int $precision = null) : DataType
{
    return DataType::timestamptz($precision);
}

/**
 * Create an INTERVAL data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_interval() : DataType
{
    return DataType::interval();
}

/**
 * Create a NUMERIC data type.
 *
 * @param null|int $precision Total number of digits
 * @param null|int $scale Number of digits after decimal point
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_numeric(?int $precision = null, ?int $scale = null) : DataType
{
    return DataType::numeric($precision, $scale);
}

/**
 * Create a DECIMAL data type (alias for NUMERIC).
 *
 * @param null|int $precision Total number of digits
 * @param null|int $scale Number of digits after decimal point
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_decimal(?int $precision = null, ?int $scale = null) : DataType
{
    return DataType::decimal($precision, $scale);
}

/**
 * Create a REAL data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_real() : DataType
{
    return DataType::real();
}

/**
 * Create a DOUBLE PRECISION data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_double() : DataType
{
    return DataType::doublePrecision();
}

/**
 * Create a UUID data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_uuid() : DataType
{
    return DataType::uuid();
}

/**
 * Create a JSON data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_json() : DataType
{
    return DataType::json();
}

/**
 * Create a JSONB data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_jsonb() : DataType
{
    return DataType::jsonb();
}

/**
 * Create a BYTEA data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_bytea() : DataType
{
    return DataType::bytea();
}

/**
 * Create an INET data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_inet() : DataType
{
    return DataType::inet();
}

/**
 * Create a CIDR data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_cidr() : DataType
{
    return DataType::cidr();
}

/**
 * Create a MACADDR data type.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_macaddr() : DataType
{
    return DataType::macaddr();
}

/**
 * Create an ARRAY data type.
 *
 * @param DataType $elementType The type of array elements
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_type_array(DataType $elementType) : DataType
{
    return DataType::array($elementType);
}

// ----------------------------------------------------------------------------
// Column Definitions
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Table Constraints
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Table DDL Commands
// ----------------------------------------------------------------------------

/**
 * Create a CREATE TABLE builder.
 *
 * Supports dot notation for schema-qualified names: "public.users" or explicit schema parameter.
 * Double-quoted identifiers preserve dots: '"my.table"' creates a single identifier.
 *
 * @param string $table Table name (may include schema as "schema.table")
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_table(string $table, ?string $schema = null) : CreateTableColumnsStep
{
    if ($schema !== null) {
        return CreateTableBuilder::create($table, $schema);
    }

    $identifier = QualifiedIdentifier::parse($table);

    return CreateTableBuilder::create($identifier->name(), $identifier->schema());
}

/**
 * Create a CREATE TABLE AS builder.
 *
 * Supports dot notation for schema-qualified names: "public.users" or explicit schema parameter.
 * Double-quoted identifiers preserve dots: '"my.table"' creates a single identifier.
 *
 * @param string $table Table name (may include schema as "schema.table")
 * @param SelectFinalStep $query SELECT query to populate the table
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_table_as(string $table, SelectFinalStep $query, ?string $schema = null) : CreateTableAsFinalStep
{
    if ($schema !== null) {
        return CreateTableAsBuilder::create($table, $query, $schema);
    }

    $identifier = QualifiedIdentifier::parse($table);

    return CreateTableAsBuilder::create($identifier->name(), $query, $identifier->schema());
}

/**
 * Create an ALTER TABLE builder.
 *
 * Supports dot notation for schema-qualified names: "public.users" or explicit schema parameter.
 * Double-quoted identifiers preserve dots: '"my.table"' creates a single identifier.
 *
 * @param string $table Table name (may include schema as "schema.table")
 * @param null|string $schema Schema name (optional, overrides parsed schema)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_table(string $table, ?string $schema = null) : AlterTableFinalStep
{
    if ($schema !== null) {
        return AlterTableBuilder::create($table, $schema);
    }

    $identifier = QualifiedIdentifier::parse($table);

    return AlterTableBuilder::create($identifier->name(), $identifier->schema());
}

/**
 * Create a DROP TABLE builder.
 *
 * @param string ...$tables Table names to drop
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_table(string ...$tables) : DropTableFinalStep
{
    return DropTableBuilder::create(...$tables);
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

// ----------------------------------------------------------------------------
// Referential Actions (for Foreign Keys)
// ----------------------------------------------------------------------------

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
 * Start building a CREATE INDEX statement.
 *
 * Use chainable methods for modifiers: ->unique(), ->concurrently(), ->ifNotExists()
 *
 * Example: create_index('idx_users_email')->unique()->on('users')->columns('email')
 *
 * @param string $name The index name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create_index(string $name) : CreateIndexOnStep
{
    return CreateIndexBuilder::create($name);
}

/**
 * Start building a DROP INDEX statement.
 *
 * Use chainable methods: ->ifExists(), ->concurrently(), ->cascade(), ->restrict()
 *
 * Example: drop_index('idx_users_email')->ifExists()->cascade()
 *
 * @param string ...$indexes The index names to drop
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop_index(string ...$indexes) : DropIndexFinalStep
{
    return DropIndexBuilder::create(...$indexes);
}

/**
 * Start building an ALTER INDEX statement.
 *
 * Use chainable methods: ->ifExists(), then ->renameTo() or ->setTablespace()
 *
 * Example: alter_index('idx_old')->ifExists()->renameTo('idx_new')
 * Example with schema: alter_index('idx_old', 'public')->renameTo('idx_new')
 *
 * @param string $name The index name
 * @param null|string $schema Optional schema name
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter_index(string $name, ?string $schema = null) : AlterIndexFinalStep
{
    return AlterIndexBuilder::create($name, $schema);
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

// ----------------------------------------------------------------------------
// Utility Commands
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Sequence Commands
// ----------------------------------------------------------------------------

/**
 * Create a CREATE SEQUENCE builder.
 *
 * Example: create_sequence('user_id_seq')->startWith(1)->incrementBy(1)
 * Produces: CREATE SEQUENCE user_id_seq START WITH 1 INCREMENT BY 1
 *
 * @param string $name Sequence name
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create_sequence(string $name, ?string $schema = null) : CreateSequenceOptionsStep
{
    return CreateSequenceBuilder::create()->sequence($name, $schema);
}

/**
 * Create an ALTER SEQUENCE builder.
 *
 * Example: alter_sequence('user_id_seq')->restartWith(1000)
 * Produces: ALTER SEQUENCE user_id_seq RESTART WITH 1000
 *
 * @param string $name Sequence name
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter_sequence(string $name, ?string $schema = null) : AlterSequenceOptionsStep
{
    return AlterSequenceBuilder::create()->sequence($name, $schema);
}

/**
 * Create a DROP SEQUENCE builder.
 *
 * Example: drop_sequence('user_id_seq', 'order_id_seq')->cascade()
 * Produces: DROP SEQUENCE user_id_seq, order_id_seq CASCADE
 *
 * @param string ...$names Sequence names to drop
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop_sequence(string ...$names) : DropSequenceFinalStep
{
    return DropSequenceBuilder::create()->sequence(...$names);
}

// ----------------------------------------------------------------------------
// View Commands
// ----------------------------------------------------------------------------

/**
 * Create a CREATE VIEW builder.
 *
 * Use chainable methods for modifiers: ->orReplace(), ->temporary(), ->recursive(), ->columns()
 *
 * Example: create_view('active_users')->as(select()->from('users')->where(eq(col('active'), literal_bool(true))))
 * Produces: CREATE VIEW active_users AS SELECT * FROM users WHERE active = true
 *
 * @param string $name View name (can include schema: schema.view)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create_view(string $name, ?string $schema = null) : CreateViewOptionsStep
{
    return CreateViewBuilder::create($name, $schema);
}

/**
 * Create a CREATE MATERIALIZED VIEW builder.
 *
 * Use chainable methods for modifiers: ->ifNotExists(), ->columns(), ->using(), ->tablespace(), ->withData(), ->withNoData()
 *
 * Example: create_materialized_view('user_stats')->as(select()->from('users'))->withNoData()
 * Produces: CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users WITH NO DATA
 *
 * @param string $name Materialized view name (can include schema: schema.matview)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function create_materialized_view(string $name, ?string $schema = null) : CreateMatViewOptionsStep
{
    return CreateMaterializedViewBuilder::create($name, $schema);
}

/**
 * Create an ALTER VIEW builder.
 *
 * Use chainable methods: ->ifExists(), then ->renameTo(), ->setSchema(), or ->ownerTo()
 *
 * Example: alter_view('old_view')->renameTo('new_view')
 * Produces: ALTER VIEW old_view RENAME TO new_view
 *
 * @param string $name View name (can include schema: schema.view)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter_view(string $name, ?string $schema = null) : AlterViewActionStep
{
    return AlterViewBuilder::create($name, $schema);
}

/**
 * Create an ALTER MATERIALIZED VIEW builder.
 *
 * Use chainable methods: ->ifExists(), then ->renameTo(), ->setSchema(), ->ownerTo(), or ->setTablespace()
 *
 * Example: alter_materialized_view('my_matview')->setTablespace('fast_storage')
 * Produces: ALTER MATERIALIZED VIEW my_matview SET TABLESPACE fast_storage
 *
 * @param string $name Materialized view name (can include schema: schema.matview)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function alter_materialized_view(string $name, ?string $schema = null) : AlterMatViewActionStep
{
    return AlterMaterializedViewBuilder::create($name, $schema);
}

/**
 * Create a DROP VIEW builder.
 *
 * Use chainable methods: ->ifExists(), ->cascade(), ->restrict()
 *
 * Example: drop_view('view1', 'view2')->ifExists()->cascade()
 * Produces: DROP VIEW IF EXISTS view1, view2 CASCADE
 *
 * @param string ...$views View names to drop
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop_view(string ...$views) : DropViewFinalStep
{
    return DropViewBuilder::create(...$views);
}

/**
 * Create a DROP MATERIALIZED VIEW builder.
 *
 * Use chainable methods: ->ifExists(), ->cascade(), ->restrict()
 *
 * Example: drop_materialized_view('matview1')->ifExists()->cascade()
 * Produces: DROP MATERIALIZED VIEW IF EXISTS matview1 CASCADE
 *
 * @param string ...$views Materialized view names to drop
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function drop_materialized_view(string ...$views) : DropMatViewFinalStep
{
    return DropMaterializedViewBuilder::create(...$views);
}

/**
 * Create a REFRESH MATERIALIZED VIEW builder.
 *
 * Use chainable methods: ->concurrently(), ->withData(), ->withNoData()
 *
 * Example: refresh_materialized_view('user_stats')->concurrently()->withData()
 * Produces: REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA
 *
 * @param string $name Materialized view name (can include schema: schema.matview)
 * @param null|string $schema Schema name (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::SCHEMA)]
function refresh_materialized_view(string $name, ?string $schema = null) : RefreshMatViewOptionsStep
{
    return RefreshMaterializedViewBuilder::create($name, $schema);
}

// ----------------------------------------------------------------------------
// Schema Commands
// ----------------------------------------------------------------------------

/**
 * Create a CREATE SCHEMA builder.
 *
 * Example: create_schema('my_schema')
 * Produces: CREATE SCHEMA my_schema
 *
 * Example: create_schema('my_schema')->ifNotExists()->authorization('admin')
 * Produces: CREATE SCHEMA IF NOT EXISTS my_schema AUTHORIZATION admin
 *
 * @param string $name The schema name
 *
 * @return CreateSchemaOptionsStep Builder for schema creation options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_schema(string $name) : CreateSchemaOptionsStep
{
    return CreateSchemaBuilder::create($name);
}

/**
 * Create an ALTER SCHEMA builder.
 *
 * Example: alter_schema('my_schema')->renameTo('new_schema')
 * Produces: ALTER SCHEMA my_schema RENAME TO new_schema
 *
 * Example: alter_schema('my_schema')->ownerTo('new_owner')
 * Produces: ALTER SCHEMA my_schema OWNER TO new_owner
 *
 * @param string $name The schema name
 *
 * @return AlterSchemaActionStep Builder for schema alter actions
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_schema(string $name) : AlterSchemaActionStep
{
    return AlterSchemaBuilder::create($name);
}

/**
 * Create a DROP SCHEMA builder.
 *
 * Example: drop_schema('my_schema')
 * Produces: DROP SCHEMA my_schema
 *
 * Example: drop_schema('schema1', 'schema2')->ifExists()->cascade()
 * Produces: DROP SCHEMA IF EXISTS schema1, schema2 CASCADE
 *
 * @param string ...$names The schema name(s) to drop
 *
 * @return DropSchemaFinalStep Builder for schema drop options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_schema(string ...$names) : DropSchemaFinalStep
{
    return DropSchemaBuilder::create(...$names);
}

// ----------------------------------------------------------------------------
// Role Commands
// ----------------------------------------------------------------------------

/**
 * Create a CREATE ROLE builder.
 *
 * Example: create_role('admin')
 * Produces: CREATE ROLE admin
 *
 * Example: create_role('admin')->superuser()->login()->withPassword('secret')
 * Produces: CREATE ROLE admin SUPERUSER LOGIN PASSWORD 'secret'
 *
 * To create a user (role with LOGIN), use: create_role('user')->login()
 *
 * @param string $name The role name
 *
 * @return CreateRoleOptionsStep Builder for role creation options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_role(string $name) : CreateRoleOptionsStep
{
    return CreateRoleBuilder::create($name);
}

/**
 * Create an ALTER ROLE builder.
 *
 * Example: alter_role('admin')->superuser()
 * Produces: ALTER ROLE admin SUPERUSER
 *
 * Example: alter_role('admin')->renameTo('administrator')
 * Produces: ALTER ROLE admin RENAME TO administrator
 *
 * @param string $name The role name
 *
 * @return AlterRoleActionStep Builder for role alter actions
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_role(string $name) : AlterRoleActionStep
{
    return AlterRoleBuilder::create($name);
}

/**
 * Create a DROP ROLE builder.
 *
 * Example: drop_role('admin')
 * Produces: DROP ROLE admin
 *
 * Example: drop_role('user1', 'user2')->ifExists()
 * Produces: DROP ROLE IF EXISTS user1, user2
 *
 * @param string ...$names The role name(s) to drop
 *
 * @return DropRoleFinalStep Builder for role drop options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_role(string ...$names) : DropRoleFinalStep
{
    return DropRoleBuilder::create(...$names);
}

// ----------------------------------------------------------------------------
// Grant/Revoke Commands
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Session Commands
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Ownership Commands
// ----------------------------------------------------------------------------

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

// =====================================================
// Function and Procedure Query Builders
// =====================================================

/**
 * Creates a new function argument for use in function/procedure definitions.
 *
 * Example: func_arg('integer')
 * Example: func_arg('text')->named('username')
 * Example: func_arg('integer')->named('count')->default('0')
 * Example: func_arg('text')->out()
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
 * Creates a CREATE FUNCTION statement builder.
 *
 * Example: create_function('add_numbers')
 *     ->arguments(func_arg('integer')->named('a'), func_arg('integer')->named('b'))
 *     ->returns('integer')
 *     ->language('sql')
 *     ->as('SELECT a + b')
 * Produces: CREATE FUNCTION add_numbers(a integer, b integer) RETURNS integer LANGUAGE sql AS 'SELECT a + b'
 *
 * Example: create_function('get_users')
 *     ->orReplace()
 *     ->returnsTable(['id' => 'integer', 'name' => 'text'])
 *     ->language('sql')
 *     ->as('SELECT id, name FROM users')
 * Produces: CREATE OR REPLACE FUNCTION get_users() RETURNS TABLE(id integer, name text) LANGUAGE sql AS '...'
 *
 * @param string $name The name of the function to create
 *
 * @return CreateFunctionArgsStep Builder for create function options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_function(string $name) : CreateFunctionArgsStep
{
    return CreateFunctionBuilder::create($name);
}

/**
 * Creates a CREATE PROCEDURE statement builder.
 *
 * Example: create_procedure('update_stats')
 *     ->arguments(func_arg('integer')->named('user_id'))
 *     ->language('plpgsql')
 *     ->as('BEGIN UPDATE user_stats SET last_updated = now() WHERE id = user_id; END;')
 * Produces: CREATE PROCEDURE update_stats(user_id integer) LANGUAGE plpgsql AS '...'
 *
 * Example: create_procedure('my_proc')->orReplace()->language('sql')->as('...')
 * Produces: CREATE OR REPLACE PROCEDURE my_proc() LANGUAGE sql AS '...'
 *
 * @param string $name The name of the procedure to create
 *
 * @return CreateProcedureArgsStep Builder for create procedure options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_procedure(string $name) : CreateProcedureArgsStep
{
    return CreateProcedureBuilder::create($name);
}

/**
 * Creates an ALTER FUNCTION statement builder.
 *
 * Example: alter_function('my_func')
 *     ->arguments(func_arg('integer'))
 *     ->immutable()
 * Produces: ALTER FUNCTION my_func(integer) IMMUTABLE
 *
 * Example: alter_function('old_name')
 *     ->arguments(func_arg('text'))
 *     ->renameTo('new_name')
 * Produces: ALTER FUNCTION old_name(text) RENAME TO new_name
 *
 * @param string $name The name of the function to alter
 *
 * @return AlterFunctionArgsStep Builder for alter function options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_function(string $name) : AlterFunctionArgsStep
{
    return AlterFunctionBuilder::create($name);
}

/**
 * Creates an ALTER PROCEDURE statement builder.
 *
 * Example: alter_procedure('my_proc')
 *     ->arguments(func_arg('integer'))
 *     ->securityDefiner()
 * Produces: ALTER PROCEDURE my_proc(integer) SECURITY DEFINER
 *
 * Example: alter_procedure('old_proc')
 *     ->renameTo('new_proc')
 * Produces: ALTER PROCEDURE old_proc RENAME TO new_proc
 *
 * @param string $name The name of the procedure to alter
 *
 * @return AlterProcedureArgsStep Builder for alter procedure options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_procedure(string $name) : AlterProcedureArgsStep
{
    return AlterProcedureBuilder::create($name);
}

/**
 * Creates a DROP FUNCTION statement builder.
 *
 * Example: drop_function('my_func')
 * Produces: DROP FUNCTION my_func
 *
 * Example: drop_function('my_func')
 *     ->ifExists()
 *     ->arguments(func_arg('integer'), func_arg('text'))
 *     ->cascade()
 * Produces: DROP FUNCTION IF EXISTS my_func(integer, text) CASCADE
 *
 * @param string $name The name of the function to drop
 *
 * @return DropFunctionFinalStep Builder for drop function options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_function(string $name) : DropFunctionFinalStep
{
    return DropFunctionBuilder::create($name);
}

/**
 * Creates a DROP PROCEDURE statement builder.
 *
 * Example: drop_procedure('my_proc')
 * Produces: DROP PROCEDURE my_proc
 *
 * Example: drop_procedure('my_proc')
 *     ->ifExists()
 *     ->arguments(func_arg('integer'))
 *     ->cascade()
 * Produces: DROP PROCEDURE IF EXISTS my_proc(integer) CASCADE
 *
 * @param string $name The name of the procedure to drop
 *
 * @return DropProcedureFinalStep Builder for drop procedure options
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_procedure(string $name) : DropProcedureFinalStep
{
    return DropProcedureBuilder::create($name);
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
 * Creates a CREATE TRIGGER statement builder.
 *
 * Example: create_trigger('audit_trigger')->before()->insert()->on('users')->execute('audit_function')
 * Produces: CREATE TRIGGER audit_trigger BEFORE INSERT ON users EXECUTE FUNCTION audit_function()
 *
 * Example: create_trigger('notify_trigger')->orReplace()->after()->insertOrUpdate()->on('orders')
 *          ->forEachRow()->when('NEW.status IS DISTINCT FROM OLD.status')->execute('notify_function')
 * Produces: CREATE OR REPLACE TRIGGER notify_trigger AFTER INSERT OR UPDATE ON orders
 *           FOR EACH ROW WHEN (NEW.status IS DISTINCT FROM OLD.status) EXECUTE FUNCTION notify_function()
 *
 * @param string $name The name of the trigger
 *
 * @return CreateTriggerTimingStep Builder for CREATE TRIGGER statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_trigger(string $name) : CreateTriggerTimingStep
{
    return CreateTriggerBuilder::create($name);
}

/**
 * Creates an ALTER TRIGGER statement builder for renaming triggers or managing extension dependencies.
 *
 * Example: alter_trigger('old_trigger')->on('users')->renameTo('new_trigger')
 * Produces: ALTER TRIGGER old_trigger ON users RENAME TO new_trigger
 *
 * Example: alter_trigger('my_trigger')->on('users')->dependsOnExtension('myext')
 * Produces: ALTER TRIGGER my_trigger ON users DEPENDS ON EXTENSION myext
 *
 * @param string $name The name of the trigger to alter
 *
 * @return AlterTriggerOnStep Builder for ALTER TRIGGER statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_trigger(string $name) : AlterTriggerOnStep
{
    return AlterTriggerBuilder::create($name);
}

/**
 * Creates a DROP TRIGGER statement builder.
 *
 * Example: drop_trigger('audit_trigger')->on('users')
 * Produces: DROP TRIGGER audit_trigger ON users
 *
 * Example: drop_trigger('audit_trigger')->ifExists()->on('public.users')->cascade()
 * Produces: DROP TRIGGER IF EXISTS audit_trigger ON public.users CASCADE
 *
 * @param string $name The name of the trigger to drop
 *
 * @return DropTriggerOnStep Builder for DROP TRIGGER statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_trigger(string $name) : DropTriggerOnStep
{
    return DropTriggerBuilder::create($name);
}

/**
 * Creates a CREATE RULE statement builder.
 *
 * Example: create_rule('prevent_delete')->asOnDelete()->to('users')->doNothing()
 * Produces: CREATE RULE prevent_delete AS ON DELETE TO users DO NOTHING
 *
 * Example: create_rule('audit_insert')->orReplace()->asOnInsert()->to('orders')
 *          ->doAlso("INSERT INTO audit_log (action, table_name) VALUES ('INSERT', 'orders')")
 * Produces: CREATE OR REPLACE RULE audit_insert AS ON INSERT TO orders
 *           DO ALSO INSERT INTO audit_log (action, table_name) VALUES ('INSERT', 'orders')
 *
 * @param string $name The name of the rule
 *
 * @return CreateRuleEventStep Builder for CREATE RULE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_rule(string $name) : CreateRuleEventStep
{
    return CreateRuleBuilder::create($name);
}

/**
 * Creates a DROP RULE statement builder.
 *
 * Example: drop_rule('prevent_delete')->on('users')
 * Produces: DROP RULE prevent_delete ON users
 *
 * Example: drop_rule('audit_insert')->ifExists()->on('public.orders')->cascade()
 * Produces: DROP RULE IF EXISTS audit_insert ON public.orders CASCADE
 *
 * @param string $name The name of the rule to drop
 *
 * @return DropRuleOnStep Builder for DROP RULE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_rule(string $name) : DropRuleOnStep
{
    return DropRuleBuilder::create($name);
}

/**
 * Creates a CREATE EXTENSION statement builder.
 *
 * Example: create_extension('uuid-ossp')
 * Produces: CREATE EXTENSION "uuid-ossp"
 *
 * Example: create_extension('postgis')->ifNotExists()->schema('public')->version('3.0')
 * Produces: CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public VERSION '3.0'
 *
 * @param string $name The name of the extension to create
 *
 * @return CreateExtensionOptionsStep Builder for CREATE EXTENSION statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_extension(string $name) : CreateExtensionOptionsStep
{
    return CreateExtensionBuilder::create($name);
}

/**
 * Creates an ALTER EXTENSION statement builder.
 *
 * Example: alter_extension('postgis')->update()
 * Produces: ALTER EXTENSION postgis UPDATE
 *
 * Example: alter_extension('postgis')->updateTo('3.1')
 * Produces: ALTER EXTENSION postgis UPDATE TO '3.1'
 *
 * Example: alter_extension('postgis')->addTable('spatial_ref_sys')
 * Produces: ALTER EXTENSION postgis ADD TABLE spatial_ref_sys
 *
 * @param string $name The name of the extension to alter
 *
 * @return AlterExtensionActionStep Builder for ALTER EXTENSION statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_extension(string $name) : AlterExtensionActionStep
{
    return AlterExtensionBuilder::create($name);
}

/**
 * Creates a DROP EXTENSION statement builder.
 *
 * Example: drop_extension('uuid-ossp')
 * Produces: DROP EXTENSION "uuid-ossp"
 *
 * Example: drop_extension('postgis', 'pg_trgm')->ifExists()->cascade()
 * Produces: DROP EXTENSION IF EXISTS postgis, pg_trgm CASCADE
 *
 * @param string ...$names The names of the extensions to drop
 *
 * @return DropExtensionFinalStep Builder for DROP EXTENSION statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_extension(string ...$names) : DropExtensionFinalStep
{
    return DropExtensionBuilder::create(...$names);
}

/**
 * Creates a CREATE TYPE (composite) statement builder.
 *
 * Example: create_composite_type('address')->attributes(type_attr('street', 'text'), type_attr('city', 'text'))
 * Produces: CREATE TYPE address AS (street text, city text)
 *
 * Example: create_composite_type('public.person')->attributes(type_attr('name', 'text')->collate('en_US'))
 * Produces: CREATE TYPE public.person AS (name text COLLATE "en_US")
 *
 * @param string $name The name of the composite type to create (can be schema-qualified)
 *
 * @return CreateCompositeTypeAttributesStep Builder for CREATE TYPE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_composite_type(string $name) : CreateCompositeTypeAttributesStep
{
    return CreateCompositeTypeBuilder::create($name);
}

/**
 * Creates a CREATE TYPE (enum) statement builder.
 *
 * Example: create_enum_type('status')->labels('pending', 'active', 'closed')
 * Produces: CREATE TYPE status AS ENUM ('pending', 'active', 'closed')
 *
 * Example: create_enum_type('public.priority')->labels('low', 'medium', 'high')
 * Produces: CREATE TYPE public.priority AS ENUM ('low', 'medium', 'high')
 *
 * @param string $name The name of the enum type to create (can be schema-qualified)
 *
 * @return CreateEnumTypeLabelsStep Builder for CREATE TYPE AS ENUM statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_enum_type(string $name) : CreateEnumTypeLabelsStep
{
    return CreateEnumTypeBuilder::create($name);
}

/**
 * Creates a CREATE TYPE (range) statement builder.
 *
 * Example: create_range_type('floatrange')->subtype('float8')
 * Produces: CREATE TYPE floatrange AS RANGE (SUBTYPE = float8)
 *
 * Example: create_range_type('daterange')->subtype('date')->subtypeOpclass('date_ops')
 * Produces: CREATE TYPE daterange AS RANGE (SUBTYPE = date, SUBTYPE_OPCLASS = date_ops)
 *
 * @param string $name The name of the range type to create (can be schema-qualified)
 *
 * @return CreateRangeTypeSubtypeStep Builder for CREATE TYPE AS RANGE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_range_type(string $name) : CreateRangeTypeSubtypeStep
{
    return CreateRangeTypeBuilder::create($name);
}

/**
 * Creates a type attribute for composite types.
 *
 * Example: type_attr('name', 'text')
 * Produces: name text
 *
 * Example: type_attr('description', 'text')->collate('en_US')
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
 * Creates an ALTER TYPE (enum) statement builder.
 *
 * Example: alter_enum_type('status')->addValue('archived')
 * Produces: ALTER TYPE status ADD VALUE 'archived'
 *
 * Example: alter_enum_type('status')->addValueBefore('pending', 'draft')
 * Produces: ALTER TYPE status ADD VALUE 'pending' BEFORE 'draft'
 *
 * Example: alter_enum_type('status')->renameValue('old_name', 'new_name')
 * Produces: ALTER TYPE status RENAME VALUE 'old_name' TO 'new_name'
 *
 * @param string $name The name of the enum type to alter (can be schema-qualified)
 *
 * @return AlterEnumTypeActionStep Builder for ALTER TYPE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_enum_type(string $name) : AlterEnumTypeActionStep
{
    return AlterEnumTypeBuilder::create($name);
}

/**
 * Creates a DROP TYPE statement builder.
 *
 * Example: drop_type('address')
 * Produces: DROP TYPE address
 *
 * Example: drop_type('status', 'priority')->ifExists()->cascade()
 * Produces: DROP TYPE IF EXISTS status, priority CASCADE
 *
 * @param string ...$names The names of the types to drop (can be schema-qualified)
 *
 * @return DropTypeFinalStep Builder for DROP TYPE statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_type(string ...$names) : DropTypeFinalStep
{
    return DropTypeBuilder::create(...$names);
}

/**
 * Creates a CREATE DOMAIN statement builder.
 *
 * Example: create_domain('email')->as('text')->constraint('valid_email')->check("VALUE ~ '^.+@.+$'")
 * Produces: CREATE DOMAIN email AS text CONSTRAINT valid_email CHECK (VALUE ~ '^.+@.+$')
 *
 * Example: create_domain('positive_int')->as('integer')->notNull()->default('0')->check('VALUE > 0')
 * Produces: CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 0 CHECK (VALUE > 0)
 *
 * @param string $name The name of the domain to create (can be schema-qualified)
 *
 * @return CreateDomainTypeStep Builder for CREATE DOMAIN statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function create_domain(string $name) : CreateDomainTypeStep
{
    return CreateDomainBuilder::create($name);
}

/**
 * Creates an ALTER DOMAIN statement builder.
 *
 * Example: alter_domain('email')->setNotNull()
 * Produces: ALTER DOMAIN email SET NOT NULL
 *
 * Example: alter_domain('email')->dropConstraint('valid_email')
 * Produces: ALTER DOMAIN email DROP CONSTRAINT valid_email
 *
 * Example: alter_domain('positive_int')->addConstraint('min_value', 'VALUE >= 0')
 * Produces: ALTER DOMAIN positive_int ADD CONSTRAINT min_value CHECK (VALUE >= 0)
 *
 * @param string $name The name of the domain to alter (can be schema-qualified)
 *
 * @return AlterDomainActionStep Builder for ALTER DOMAIN statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function alter_domain(string $name) : AlterDomainActionStep
{
    return AlterDomainBuilder::create($name);
}

/**
 * Creates a DROP DOMAIN statement builder.
 *
 * Example: drop_domain('email')
 * Produces: DROP DOMAIN email
 *
 * Example: drop_domain('email', 'positive_int')->ifExists()->cascade()
 * Produces: DROP DOMAIN IF EXISTS email, positive_int CASCADE
 *
 * @param string ...$names The names of the domains to drop (can be schema-qualified)
 *
 * @return DropDomainFinalStep Builder for DROP DOMAIN statement
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function drop_domain(string ...$names) : DropDomainFinalStep
{
    return DropDomainBuilder::create(...$names);
}
