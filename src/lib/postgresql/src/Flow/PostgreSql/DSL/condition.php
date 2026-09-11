<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\All;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\Any;
use Flow\PostgreSql\QueryBuilder\Condition\Between;
use Flow\PostgreSql\QueryBuilder\Condition\BooleanCondition;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionBuilder;
use Flow\PostgreSql\QueryBuilder\Condition\Exists;
use Flow\PostgreSql\QueryBuilder\Condition\In;
use Flow\PostgreSql\QueryBuilder\Condition\IsDistinctFrom;
use Flow\PostgreSql\QueryBuilder\Condition\IsNull;
use Flow\PostgreSql\QueryBuilder\Condition\Like;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OperatorCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Condition\SimilarTo;
use Flow\PostgreSql\QueryBuilder\Expression\BinaryExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\Subquery;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use InvalidArgumentException;

use function array_values;
use function count;

/**
 * Create an equality comparison (column = value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function eq(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::EQ,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a not-equal comparison (column != value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ne(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::NEQ,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a less-than comparison (column < value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function lt(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::LT,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a less-than-or-equal comparison (column <= value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function le(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::LTE,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a greater-than comparison (column > value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function gt(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::GT,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a greater-than-or-equal comparison (column >= value).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function ge(string|Expression $left, string|Expression $right): Comparison
{
    return new Comparison(
        $left instanceof Expression ? $left : col($left),
        ComparisonOperator::GTE,
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a BETWEEN condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function between(string|Expression $expr, string|Expression $low, string|Expression $high, bool $not = false): Between
{
    return new Between(
        $expr instanceof Expression ? $expr : col($expr),
        $low instanceof Expression ? $low : col($low),
        $high instanceof Expression ? $high : col($high),
        $not,
    );
}

/**
 * Create an IN condition.
 *
 * @param Expression|string $expr Expression to check
 * @param list<Expression> $values List of values (must be non-empty)
 *
 * @throws \InvalidArgumentException when values array is empty
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function in_(string|Expression $expr, array $values): In
{
    $values = array_values($values);

    if (count($values) === 1 && $values[0] instanceof Subquery) {
        throw new InvalidArgumentException(
            'in_() does not support subqueries; use any_($expr, ComparisonOperator::EQ, $subquery) for "IN (subquery)" semantics.',
        );
    }

    return new In($expr instanceof Expression ? $expr : col($expr), $values);
}

/**
 * Create an IS NULL condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function is_null(string|Expression $expr, bool $not = false): IsNull
{
    return new IsNull($expr instanceof Expression ? $expr : col($expr), $not);
}

/**
 * Create a LIKE condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function like(
    string|Expression $expr,
    string|Expression $pattern,
    bool $caseInsensitive = false,
    bool $negated = false,
): Like {
    return new Like(
        $expr instanceof Expression ? $expr : col($expr),
        $pattern instanceof Expression ? $pattern : col($pattern),
        $caseInsensitive,
        negated: $negated,
    );
}

/**
 * Create a SIMILAR TO condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function similar_to(string|Expression $expr, string|Expression $pattern): SimilarTo
{
    return new SimilarTo(
        $expr instanceof Expression ? $expr : col($expr),
        $pattern instanceof Expression ? $pattern : col($pattern),
    );
}

/**
 * Create an IS DISTINCT FROM condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function distinct_from(string|Expression $left, string|Expression $right, bool $not = false): IsDistinctFrom
{
    return new IsDistinctFrom(
        $left instanceof Expression ? $left : col($left),
        $right instanceof Expression ? $right : col($right),
        $not,
    );
}

/**
 * Create an EXISTS condition.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function exists(SelectFinalStep $subquery): Exists
{
    $node = new Node();
    $node->setSelectStmt($subquery->toAst());

    return new Exists($node);
}

/**
 * Create an ANY condition with a subquery or array expression.
 *
 * Example: any_(col('id'), ComparisonOperator::EQ, select(col('user_id'))->from(table('orders')))
 * Example: any_(col('attnum', 'a'), ComparisonOperator::EQ, col('conkey', 'con'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function any_(string|Expression $left, ComparisonOperator $operator, Expression|SelectFinalStep $arrayOrSubquery): Any
{
    $left = $left instanceof Expression ? $left : col($left);

    if ($arrayOrSubquery instanceof SelectFinalStep) {
        $node = new Node(['select_stmt' => $arrayOrSubquery->toAst()]);

        return new Any($left, $operator, $node);
    }

    return new Any($left, $operator, $arrayOrSubquery);
}

/**
 * Create an ALL condition with a subquery or array expression.
 *
 * Example: all_(col('id'), ComparisonOperator::EQ, select(col('user_id'))->from(table('orders')))
 * Example: all_(col('value'), ComparisonOperator::GT, col('thresholds'))
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function all_(string|Expression $left, ComparisonOperator $operator, Expression|SelectFinalStep $arrayOrSubquery): All
{
    $left = $left instanceof Expression ? $left : col($left);

    if ($arrayOrSubquery instanceof SelectFinalStep) {
        $node = new Node(['select_stmt' => $arrayOrSubquery->toAst()]);

        return new All($left, $operator, $node);
    }

    return new All($left, $operator, $arrayOrSubquery);
}

/**
 * Wrap an expression as a boolean condition for use in WHERE/HAVING/JOIN ON.
 *
 * Example: is_true(col('is_active')) — uses a boolean column in WHERE clause.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function is_true(string|Expression $expr): BooleanCondition
{
    return new BooleanCondition($expr instanceof Expression ? $expr : col($expr));
}

/**
 * Create a NOT LIKE condition.
 *
 * Example: not_like(col('name'), literal('pg_%'))
 * Produces: name NOT LIKE 'pg_%'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function not_like(string|Expression $expr, string|Expression $pattern, bool $caseInsensitive = false): Like
{
    return new Like(
        $expr instanceof Expression ? $expr : col($expr),
        $pattern instanceof Expression ? $pattern : col($pattern),
        $caseInsensitive,
        negated: true,
    );
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
function conditions(): ConditionBuilder
{
    return ConditionBuilder::create();
}

/**
 * Combine conditions with AND.
 *
 * @param Condition ...$conditions Conditions to combine
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function and_(Condition ...$conditions): AndCondition
{
    return new AndCondition(...$conditions);
}

/**
 * Combine conditions with OR.
 *
 * @param Condition ...$conditions Conditions to combine
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function or_(Condition ...$conditions): OrCondition
{
    return new OrCondition(...$conditions);
}

/**
 * Negate a condition or expression with NOT.
 *
 * Accepts both Condition and Expression — NOT always produces a boolean result.
 * Can be used in WHERE clauses and SELECT lists (via ->as('alias')).
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function not_(string|Expression $expression): NotCondition
{
    return new NotCondition($expression instanceof Expression ? $expression : col($expression));
}

/**
 * Create a JSONB contains condition (@>).
 *
 * Example: json_contains(col('metadata'), literal_json('{"category": "electronics"}'))
 * Produces: metadata @> '{"category": "electronics"}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_contains(string|Expression $left, string|Expression $right): OperatorCondition
{
    return new OperatorCondition(
        $left instanceof Expression ? $left : col($left),
        '@>',
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a JSONB is contained by condition (<@).
 *
 * Example: json_contained_by(col('metadata'), literal_json('{"category": "electronics", "price": 100}'))
 * Produces: metadata <@ '{"category": "electronics", "price": 100}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_contained_by(string|Expression $left, string|Expression $right): OperatorCondition
{
    return new OperatorCondition(
        $left instanceof Expression ? $left : col($left),
        '<@',
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create a JSON field access expression (->).
 * Returns JSON.
 *
 * Example: json_get(col('metadata'), literal_string('category'))
 * Produces: metadata -> 'category'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_get(string|Expression $expr, string|Expression $key): BinaryExpression
{
    return new BinaryExpression(
        $expr instanceof Expression ? $expr : col($expr),
        '->',
        $key instanceof Expression ? $key : col($key),
    );
}

/**
 * Create a JSON field access expression (->>).
 * Returns text.
 *
 * Example: json_get_text(col('metadata'), literal_string('name'))
 * Produces: metadata ->> 'name'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_get_text(string|Expression $expr, string|Expression $key): BinaryExpression
{
    return new BinaryExpression(
        $expr instanceof Expression ? $expr : col($expr),
        '->>',
        $key instanceof Expression ? $key : col($key),
    );
}

/**
 * Create a JSON path access expression (#>).
 * Returns JSON.
 *
 * Example: json_path(col('metadata'), literal_string('{category,name}'))
 * Produces: metadata #> '{category,name}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_path(string|Expression $expr, string|Expression $path): BinaryExpression
{
    return new BinaryExpression(
        $expr instanceof Expression ? $expr : col($expr),
        '#>',
        $path instanceof Expression ? $path : col($path),
    );
}

/**
 * Create a JSON path access expression (#>>).
 * Returns text.
 *
 * Example: json_path_text(col('metadata'), literal_string('{category,name}'))
 * Produces: metadata #>> '{category,name}'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_path_text(string|Expression $expr, string|Expression $path): BinaryExpression
{
    return new BinaryExpression(
        $expr instanceof Expression ? $expr : col($expr),
        '#>>',
        $path instanceof Expression ? $path : col($path),
    );
}

/**
 * Create a JSONB key exists condition (?).
 *
 * Example: json_exists(col('metadata'), literal_string('category'))
 * Produces: metadata ? 'category'
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists(string|Expression $expr, string|Expression $key): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '?',
        $key instanceof Expression ? $key : col($key),
    );
}

/**
 * Create a JSONB any key exists condition (?|).
 *
 * Example: json_exists_any(col('metadata'), array_expr([literal('category'), literal('name')]))
 * Produces: metadata ?| array['category', 'name']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists_any(string|Expression $expr, string|Expression $keys): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '?|',
        $keys instanceof Expression ? $keys : col($keys),
    );
}

/**
 * Create a JSONB all keys exist condition (?&).
 *
 * Example: json_exists_all(col('metadata'), array_expr([literal('category'), literal('name')]))
 * Produces: metadata ?& array['category', 'name']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function json_exists_all(string|Expression $expr, string|Expression $keys): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '?&',
        $keys instanceof Expression ? $keys : col($keys),
    );
}

/**
 * Create an array contains condition (@>).
 *
 * Example: array_contains(col('tags'), array_expr([literal('sale')]))
 * Produces: tags @> ARRAY['sale']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_contains(string|Expression $left, string|Expression $right): OperatorCondition
{
    return new OperatorCondition(
        $left instanceof Expression ? $left : col($left),
        '@>',
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create an array is contained by condition (<@).
 *
 * Example: array_contained_by(col('tags'), array_expr([literal('sale'), literal('featured'), literal('new')]))
 * Produces: tags <@ ARRAY['sale', 'featured', 'new']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_contained_by(string|Expression $left, string|Expression $right): OperatorCondition
{
    return new OperatorCondition(
        $left instanceof Expression ? $left : col($left),
        '<@',
        $right instanceof Expression ? $right : col($right),
    );
}

/**
 * Create an array overlap condition (&&).
 *
 * Example: array_overlap(col('tags'), array_expr([literal('sale'), literal('featured')]))
 * Produces: tags && ARRAY['sale', 'featured']
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function array_overlap(string|Expression $left, string|Expression $right): OperatorCondition
{
    return new OperatorCondition(
        $left instanceof Expression ? $left : col($left),
        '&&',
        $right instanceof Expression ? $right : col($right),
    );
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
function regex_match(string|Expression $expr, string|Expression $pattern): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '~',
        $pattern instanceof Expression ? $pattern : col($pattern),
    );
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
function regex_imatch(string|Expression $expr, string|Expression $pattern): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '~*',
        $pattern instanceof Expression ? $pattern : col($pattern),
    );
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
function not_regex_match(string|Expression $expr, string|Expression $pattern): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '!~',
        $pattern instanceof Expression ? $pattern : col($pattern),
    );
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
function not_regex_imatch(string|Expression $expr, string|Expression $pattern): OperatorCondition
{
    return new OperatorCondition(
        $expr instanceof Expression ? $expr : col($expr),
        '!~*',
        $pattern instanceof Expression ? $pattern : col($pattern),
    );
}

/**
 * Create a full-text search match condition (@@).
 *
 * Example: text_search_match(col('document'), func('to_tsquery', [literal('english'), literal('hello & world')]))
 * Produces: document @@ to_tsquery('english', 'hello & world')
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function text_search_match(string|Expression $document, string|Expression $query): OperatorCondition
{
    return new OperatorCondition(
        $document instanceof Expression ? $document : col($document),
        '@@',
        $query instanceof Expression ? $query : col($query),
    );
}
