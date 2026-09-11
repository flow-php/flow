<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\CTEMaterialization;
use Flow\PostgreSql\QueryBuilder\Clause\FrameBound;
use Flow\PostgreSql\QueryBuilder\Clause\FrameExclusion;
use Flow\PostgreSql\QueryBuilder\Clause\FrameMode;
use Flow\PostgreSql\QueryBuilder\Clause\LockingClause;
use Flow\PostgreSql\QueryBuilder\Clause\LockStrength;
use Flow\PostgreSql\QueryBuilder\Clause\LockWaitPolicy;
use Flow\PostgreSql\QueryBuilder\Clause\NullsPosition;
use Flow\PostgreSql\QueryBuilder\Clause\OnConflictClause;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\ReturningClause;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;
use Flow\PostgreSql\QueryBuilder\Clause\WindowDefinition;
use Flow\PostgreSql\QueryBuilder\Clause\WindowFrame;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Cursor\CloseCursorBuilder;
use Flow\PostgreSql\QueryBuilder\Cursor\CloseCursorFinalStep;
use Flow\PostgreSql\QueryBuilder\Cursor\DeclareCursorBuilder;
use Flow\PostgreSql\QueryBuilder\Cursor\DeclareCursorOptionsStep;
use Flow\PostgreSql\QueryBuilder\Cursor\FetchCursorBuilder;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteBuilder;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteFromStep;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\AggregateCall;
use Flow\PostgreSql\QueryBuilder\Expression\ArrayExpression;
use Flow\PostgreSql\QueryBuilder\Expression\BinaryExpression;
use Flow\PostgreSql\QueryBuilder\Expression\CaseExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Coalesce;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Greatest;
use Flow\PostgreSql\QueryBuilder\Expression\Least;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Expression\NullIf;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use Flow\PostgreSql\QueryBuilder\Expression\RowExpression;
use Flow\PostgreSql\QueryBuilder\Expression\SQLValueFunctionExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use Flow\PostgreSql\QueryBuilder\Expression\Subquery;
use Flow\PostgreSql\QueryBuilder\Expression\TypeCast;
use Flow\PostgreSql\QueryBuilder\Expression\WhenClause;
use Flow\PostgreSql\QueryBuilder\Expression\WindowFunction;
use Flow\PostgreSql\QueryBuilder\Factory\CopyFactory;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use Flow\PostgreSql\QueryBuilder\Insert\InsertBuilder;
use Flow\PostgreSql\QueryBuilder\Insert\InsertIntoStep;
use Flow\PostgreSql\QueryBuilder\Listen\ListenBuilder;
use Flow\PostgreSql\QueryBuilder\Listen\ListenFinalStep;
use Flow\PostgreSql\QueryBuilder\Merge\MergeBuilder;
use Flow\PostgreSql\QueryBuilder\Merge\MergeUsingStep;
use Flow\PostgreSql\QueryBuilder\Notify\NotifyBuilder;
use Flow\PostgreSql\QueryBuilder\Notify\NotifyFinalStep;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Select\ParsedSelect;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectSelectStep;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\QueryBuilder\Table\DerivedTable;
use Flow\PostgreSql\QueryBuilder\Table\Lateral;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use Flow\PostgreSql\QueryBuilder\Table\TableFunction;
use Flow\PostgreSql\QueryBuilder\Table\TableReference;
use Flow\PostgreSql\QueryBuilder\Table\ValuesTable;
use Flow\PostgreSql\QueryBuilder\Transaction\BeginBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\BeginOptionsStep;
use Flow\PostgreSql\QueryBuilder\Transaction\CommitBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\CommitOptionsStep;
use Flow\PostgreSql\QueryBuilder\Transaction\PreparedTransactionBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\PreparedTransactionFinalStep;
use Flow\PostgreSql\QueryBuilder\Transaction\RollbackBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\RollbackOptionsStep;
use Flow\PostgreSql\QueryBuilder\Transaction\SavepointBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\SavepointFinalStep;
use Flow\PostgreSql\QueryBuilder\Transaction\SetTransactionBuilder;
use Flow\PostgreSql\QueryBuilder\Transaction\SetTransactionFinalStep;
use Flow\PostgreSql\QueryBuilder\Transaction\SetTransactionOptionsStep;
use Flow\PostgreSql\QueryBuilder\Unlisten\UnlistenBuilder;
use Flow\PostgreSql\QueryBuilder\Unlisten\UnlistenFinalStep;
use Flow\PostgreSql\QueryBuilder\Update\UpdateBuilder;
use Flow\PostgreSql\QueryBuilder\Update\UpdateTableStep;
use Flow\PostgreSql\QueryBuilder\With\WithBuilder;
use InvalidArgumentException;

use function array_map;
use function array_values;
use function count;
use function Flow\Types\DSL\type_string;
use function is_float;
use function is_int;
use function is_string;
use function str_contains;
use function strtolower;

/**
 * Create a new SELECT query builder.
 *
 * @param Expression|string ...$expressions Columns to select. If empty, returns SelectSelectStep.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function select(string|Expression ...$expressions): SelectBuilder
{
    if ($expressions === []) {
        return SelectBuilder::create();
    }

    $expressions = array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), $expressions);

    return SelectBuilder::create()->select(...$expressions);
}

/**
 * Create a SelectFinalStep from a raw SQL SELECT string.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function parsed_select(string $sql): ParsedSelect
{
    return new ParsedSelect($sql);
}

/**
 * Create a WITH clause builder for CTEs.
 *
 * Example: with(cte('users', $subquery))->select(star())->from(table('users'))
 * Example: with(cte('a', $q1), cte('b', $q2))->recursive()->select(...)->from(table('a'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function with(CTE ...$ctes): WithBuilder
{
    if ($ctes === []) {
        throw new InvalidArgumentException('At least one CTE is required');
    }

    return new WithBuilder(new WithClause($ctes));
}

/**
 * Create a new INSERT query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function insert(): InsertIntoStep
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
function bulk_insert(string $table, array $columns, int $rowCount): BulkInsert
{
    return BulkInsert::into($table, $columns, $rowCount);
}

/**
 * Create a new UPDATE query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function update(): UpdateTableStep
{
    return UpdateBuilder::create();
}

/**
 * Create a new DELETE query builder.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function delete(): DeleteFromStep
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
function merge(string $table, ?string $alias = null): MergeUsingStep
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
function copy(): CopyFactory
{
    return new CopyFactory();
}

/**
 * Create a LISTEN statement to subscribe the current session to a notification channel.
 *
 * Usage:
 *   listen('my_channel')->toSql()  // LISTEN my_channel
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function listen(string $channel): ListenFinalStep
{
    return ListenBuilder::create($channel);
}

/**
 * Create an UNLISTEN statement to unsubscribe the current session from a notification channel.
 *
 * Usage:
 *   unlisten('my_channel')->toSql()  // UNLISTEN my_channel
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function unlisten(string $channel): UnlistenFinalStep
{
    return UnlistenBuilder::create($channel);
}

/**
 * Create a NOTIFY statement to send a notification on a channel, optionally with a payload.
 *
 * Usage:
 *   notify('my_channel')->toSql()                           // NOTIFY my_channel
 *   notify('my_channel')->withPayload('hello')->toSql()     // NOTIFY my_channel, 'hello'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function notify(string $channel): NotifyFinalStep
{
    return NotifyBuilder::create($channel);
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
function col(string $column, ?string $table = null, ?string $schema = null): Column
{
    if ($table !== null || $schema !== null) {
        if ($schema !== null && $table === null) {
            throw new InvalidExpressionException('Cannot specify schema without table in col()');
        }

        if (str_contains($column, '.')) {
            throw new InvalidExpressionException(
                'Column name cannot contain dots when table or schema is specified. Use col("table.column") or col("column", "table") but not both.',
            );
        }

        if ($schema !== null) {
            return Column::schemaTableColumn($schema, type_string()->assert($table), $column);
        }

        return Column::tableColumn(type_string()->assert($table), $column);
    }

    return Column::fromParts(QualifiedIdentifier::parse($column)->parts());
}

/**
 * Create a SELECT * expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function star(?string $table = null): Star
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
function literal(string|int|float|bool|null $value): Literal
{
    return match (true) {
        $value === null => Literal::null(),
        is_string($value) => Literal::string($value),
        is_int($value) => Literal::int($value),
        is_float($value) => Literal::float($value),
        default => Literal::bool($value),
    };
}

/**
 * Create a positional parameter ($1, $2, etc.).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function param(int $position): Parameter
{
    return Parameter::positional($position);
}

/**
 * @return list<Parameter>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function parameters(int $count, int $startAt = 1): array
{
    if ($count < 1) {
        throw new InvalidArgumentException('Parameter count must be at least 1');
    }

    if ($startAt < 1) {
        throw new InvalidArgumentException('Start position must be at least 1');
    }

    $params = [];

    for ($i = 0; $i < $count; $i++) {
        $params[] = Parameter::positional($startAt + $i);
    }

    return $params;
}

/**
 * Create a function call expression.
 *
 * @param string $name Function name (can include schema like "pg_catalog.now")
 * @param list<Expression|string> $args Function arguments
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function func(string $name, array $args = []): FunctionCall
{
    $parts = QualifiedIdentifier::parse($name)->parts();

    if (count($parts) === 1) {
        $helper = match (strtolower($parts[0])) {
            'greatest' => 'greatest',
            'least' => 'least',
            'coalesce' => 'coalesce',
            'nullif' => 'nullif',
            'current_timestamp' => 'current_timestamp',
            'current_date' => 'current_date',
            'current_time' => 'current_time',
            default => null,
        };

        if ($helper !== null) {
            throw InvalidExpressionException::keywordConstruct($parts[0], $helper);
        }
    }

    return new FunctionCall($parts, array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), $args));
}

/**
 * Create an aggregate function call (COUNT, SUM, AVG, etc.).
 *
 * @param string $name Aggregate function name
 * @param list<Expression|string> $args Function arguments
 * @param bool $distinct Use DISTINCT modifier
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg(string $name, array $args = [], bool $distinct = false): AggregateCall
{
    return new AggregateCall(
        [$name],
        array_map(static fn(string|Expression $e): Expression => $e instanceof Expression ? $e : col($e), $args),
        false,
        $distinct,
    );
}

/**
 * Create COUNT(*) aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_count(string|Expression|null $expr = null, bool $distinct = false): AggregateCall
{
    if ($expr === null) {
        return new AggregateCall(['count'], [], true, false);
    }

    return new AggregateCall(['count'], [$expr instanceof Expression ? $expr : col($expr)], false, $distinct);
}

/**
 * Create COUNT(*) aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function count_all(): AggregateCall
{
    return new AggregateCall(['count'], [], true, false);
}

/**
 * Create SUM aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_sum(string|Expression $expr, bool $distinct = false): AggregateCall
{
    return new AggregateCall(['sum'], [$expr instanceof Expression ? $expr : col($expr)], false, $distinct);
}

/**
 * Create AVG aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_avg(string|Expression $expr, bool $distinct = false): AggregateCall
{
    return new AggregateCall(['avg'], [$expr instanceof Expression ? $expr : col($expr)], false, $distinct);
}

/**
 * Create MIN aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_min(string|Expression $expr): AggregateCall
{
    return new AggregateCall(['min'], [$expr instanceof Expression ? $expr : col($expr)]);
}

/**
 * Create MAX aggregate.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function agg_max(string|Expression $expr): AggregateCall
{
    return new AggregateCall(['max'], [$expr instanceof Expression ? $expr : col($expr)]);
}

/**
 * Create a COALESCE expression.
 *
 * @param Expression|string ...$expressions Expressions to coalesce
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function coalesce(string|Expression ...$expressions): Coalesce
{
    return new Coalesce(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($expressions)));
}

/**
 * Create a NULLIF expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function nullif(string|Expression $expr1, string|Expression $expr2): NullIf
{
    return new NullIf(
        $expr1 instanceof Expression ? $expr1 : col($expr1),
        $expr2 instanceof Expression ? $expr2 : col($expr2),
    );
}

/**
 * Create a GREATEST expression.
 *
 * @param Expression|string ...$expressions Expressions to compare
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function greatest(string|Expression ...$expressions): Greatest
{
    return new Greatest(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($expressions)));
}

/**
 * Create a LEAST expression.
 *
 * @param Expression|string ...$expressions Expressions to compare
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function least(string|Expression ...$expressions): Least
{
    return new Least(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($expressions)));
}

/**
 * Create a type cast expression.
 *
 * @param Expression|string $expr Expression to cast
 * @param ColumnType $dataType Target data type (use column_type_* functions)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function cast(string|Expression $expr, ColumnType $dataType): TypeCast
{
    return new TypeCast($expr instanceof Expression ? $expr : col($expr), $dataType);
}

/**
 * SQL standard CURRENT_TIMESTAMP function.
 *
 * Returns the current date and time (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('created_at', column_type_timestamp())->default(current_timestamp())
 * Example: select()->select(current_timestamp()->as('now'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_timestamp(): SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentTimestamp();
}

/**
 * SQL standard CURRENT_DATE function.
 *
 * Returns the current date (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('birth_date', column_type_date())->default(current_date())
 * Example: select()->select(current_date()->as('today'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_date(): SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentDate();
}

/**
 * SQL standard CURRENT_TIME function.
 *
 * Returns the current time (at the start of the transaction).
 * Useful as a column default value or in SELECT queries.
 *
 * Example: column('start_time', column_type_time())->default(current_time())
 * Example: select()->select(current_time()->as('now_time'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function current_time(): SQLValueFunctionExpression
{
    return SQLValueFunctionExpression::currentTime();
}

/**
 * Create a CASE expression.
 *
 * @param non-empty-list<WhenClause> $whenClauses WHEN clauses
 * @param null|Expression|string $elseResult ELSE result (optional)
 * @param null|Expression|string $operand CASE operand for simple CASE (optional)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function case_when(
    array $whenClauses,
    string|Expression|null $elseResult = null,
    string|Expression|null $operand = null,
): CaseExpression {
    return new CaseExpression(
        $operand === null ? null : ($operand instanceof Expression ? $operand : col($operand)),
        array_values($whenClauses),
        $elseResult === null ? null : ($elseResult instanceof Expression ? $elseResult : col($elseResult)),
    );
}

/**
 * Create a WHEN clause for CASE expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function when(string|Expression $condition, string|Expression $result): WhenClause
{
    return new WhenClause(
        $condition instanceof Expression ? $condition : col($condition),
        $result instanceof Expression ? $result : col($result),
    );
}

/**
 * Create a subquery expression.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sub_select(SelectFinalStep $query): Subquery
{
    $node = new Node();
    $node->setSelectStmt($query->toAst());

    return new Subquery($node);
}

/**
 * Create an array expression.
 *
 * @param list<Expression|string> $elements Array elements
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_expr(array $elements): ArrayExpression
{
    return new ArrayExpression(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($elements)));
}

/**
 * Create a row expression.
 *
 * @param list<Expression|string> $elements Row elements
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function row_expr(array $elements): RowExpression
{
    return new RowExpression(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($elements)));
}

/**
 * Create a binary expression (left op right).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function binary_expr(string|Expression $left, string $operator, string|Expression $right): BinaryExpression
{
    return new BinaryExpression(
        $left instanceof Expression ? $left : col($left),
        $operator,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a window function.
 *
 * @param string $name Function name
 * @param list<Expression|string> $args Function arguments
 * @param list<Expression|string> $partitionBy PARTITION BY expressions
 * @param list<OrderBy> $orderBy ORDER BY items
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function window_func(string $name, array $args = [], array $partitionBy = [], array $orderBy = []): WindowFunction
{
    $coerce = static fn(string|Expression $e): Expression => $e instanceof Expression ? $e : col($e);

    return new WindowFunction(
        [$name],
        array_map($coerce, array_values($args)),
        array_map($coerce, array_values($partitionBy)),
        array_values($orderBy),
    );
}

/**
 * Concatenate expressions with the || operator.
 *
 * Example: concat(col('schema'), literal('.'), col('table'))
 * Produces: schema || '.' || table
 *
 * @param Expression|string ...$expressions At least 2 expressions to concatenate
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function concat(string|Expression ...$expressions): BinaryExpression
{
    if (count($expressions) < 2) {
        throw InvalidExpressionException::emptyArray('concat requires at least 2 expressions');
    }

    $expressions = array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), $expressions);

    $result = $expressions[0];

    for ($i = 1; $i < count($expressions); $i++) {
        $result = new BinaryExpression($result, '||', $expressions[$i]);
    }

    /** @var BinaryExpression $result */
    return $result;
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
function table(string $name, ?string $schema = null): Table
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
function derived(SelectFinalStep $query, string $alias): DerivedTable
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
function lateral(TableReference $reference): Lateral
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
function table_func(FunctionCall $function, bool $withOrdinality = false): TableFunction
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
function values_table(RowExpression ...$rows): ValuesTable
{
    return new ValuesTable($rows);
}

/**
 * Create an ORDER BY item.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function order_by(
    string|Expression $expr,
    SortDirection $direction = SortDirection::ASC,
    NullsPosition $nulls = NullsPosition::DEFAULT,
): OrderBy {
    return new OrderBy($expr instanceof Expression ? $expr : col($expr), $direction, $nulls);
}

/**
 * Create an ORDER BY item with ASC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function asc(string|Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT): OrderBy
{
    return new OrderBy($expr instanceof Expression ? $expr : col($expr), SortDirection::ASC, $nulls);
}

/**
 * Create an ORDER BY item with DESC direction.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function desc(string|Expression $expr, NullsPosition $nulls = NullsPosition::DEFAULT): OrderBy
{
    return new OrderBy($expr instanceof Expression ? $expr : col($expr), SortDirection::DESC, $nulls);
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
): CTE {
    $node = new Node();
    $node->setSelectStmt($query->toAst());

    return new CTE($name, $node, $columnNames, $materialization, $recursive);
}

/**
 * Create a window definition for WINDOW clause.
 *
 * @param string $name Window name
 * @param list<Expression|string> $partitionBy PARTITION BY expressions
 * @param list<OrderBy> $orderBy ORDER BY items
 * @param null|WindowFrame $frame Window frame specification
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function window_def(
    string $name,
    array $partitionBy = [],
    array $orderBy = [],
    ?WindowFrame $frame = null,
): WindowDefinition {
    return new WindowDefinition(
        $name,
        array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
            ? $e
            : col($e), array_values($partitionBy)),
        array_values($orderBy),
        $frame,
    );
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
): WindowFrame {
    return new WindowFrame($mode, $start, $end, $exclusion);
}

/**
 * Create a frame bound for CURRENT ROW.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_current_row(): FrameBound
{
    return FrameBound::currentRow();
}

/**
 * Create a frame bound for UNBOUNDED PRECEDING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_unbounded_preceding(): FrameBound
{
    return FrameBound::unboundedPreceding();
}

/**
 * Create a frame bound for UNBOUNDED FOLLOWING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_unbounded_following(): FrameBound
{
    return FrameBound::unboundedFollowing();
}

/**
 * Create a frame bound for N PRECEDING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_preceding(string|Expression $offset): FrameBound
{
    return FrameBound::preceding($offset instanceof Expression ? $offset : col($offset));
}

/**
 * Create a frame bound for N FOLLOWING.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function frame_following(string|Expression $offset): FrameBound
{
    return FrameBound::following($offset instanceof Expression ? $offset : col($offset));
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
): LockingClause {
    return new LockingClause($strength, array_values($tables), $waitPolicy);
}

/**
 * Create a FOR UPDATE locking clause.
 *
 * @param list<string> $tables Tables to lock (empty for all)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function for_update(array $tables = []): LockingClause
{
    return LockingClause::forUpdate(array_values($tables));
}

/**
 * Create a FOR SHARE locking clause.
 *
 * @param list<string> $tables Tables to lock (empty for all)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function for_share(array $tables = []): LockingClause
{
    return LockingClause::forShare(array_values($tables));
}

/**
 * Create an ON CONFLICT DO NOTHING clause.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function on_conflict_nothing(?ConflictTarget $target = null): OnConflictClause
{
    return OnConflictClause::doNothing($target);
}

/**
 * Create an ON CONFLICT DO UPDATE clause.
 *
 * @param ConflictTarget $target Conflict target (columns or constraint)
 * @param array<string, Expression|string> $updates Column updates
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function on_conflict_update(ConflictTarget $target, array $updates): OnConflictClause
{
    return OnConflictClause::doUpdate($target, array_map(static fn(string|Expression $e): Expression => $e
        instanceof Expression
            ? $e
            : col($e), $updates));
}

/**
 * Create a conflict target for ON CONFLICT (columns).
 *
 * @param list<string> $columns Columns that define uniqueness
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function conflict_columns(array $columns): ConflictTarget
{
    return ConflictTarget::columns(array_values($columns));
}

/**
 * Create a conflict target for ON CONFLICT ON CONSTRAINT.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function conflict_constraint(string $name): ConflictTarget
{
    return ConflictTarget::constraint($name);
}

/**
 * Create a RETURNING clause.
 *
 * @param Expression|string ...$expressions Expressions to return
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function returning(string|Expression ...$expressions): ReturningClause
{
    return new ReturningClause(array_map(static fn(string|Expression $e): Expression => $e instanceof Expression
        ? $e
        : col($e), array_values($expressions)));
}

/**
 * Create a RETURNING * clause.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function returning_all(): ReturningClause
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
function begin(): BeginOptionsStep
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
function commit(): CommitOptionsStep
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
function rollback(): RollbackOptionsStep
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
function savepoint(string $name): SavepointFinalStep
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
function release_savepoint(string $name): SavepointFinalStep
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
function set_transaction(): SetTransactionOptionsStep
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
function set_session_transaction(): SetTransactionOptionsStep
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
function transaction_snapshot(string $snapshotId): SetTransactionFinalStep
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
function prepare_transaction(string $transactionId): PreparedTransactionFinalStep
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
function commit_prepared(string $transactionId): PreparedTransactionFinalStep
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
function rollback_prepared(string $transactionId): PreparedTransactionFinalStep
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
 * @param SelectFinalStep|Sql|string $query Query to iterate over
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function declare_cursor(string $cursorName, SelectFinalStep|string|Sql $query): DeclareCursorOptionsStep
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
function fetch(string $cursorName): FetchCursorBuilder
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
function close_cursor(?string $cursorName = null): CloseCursorFinalStep
{
    if ($cursorName === null) {
        return CloseCursorBuilder::closeAll();
    }

    return CloseCursorBuilder::close($cursorName);
}
