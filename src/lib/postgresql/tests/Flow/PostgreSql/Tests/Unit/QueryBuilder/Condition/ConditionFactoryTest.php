<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class ConditionFactoryTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public static function row_comparisons_with_subquery(): Generator
    {
        yield 'equal' => ['(a, b) = (SELECT x, y FROM u)', '(a, b) = (SELECT x, y FROM u)'];
        yield 'not equal' => ['(a, b) <> (SELECT x, y FROM u)', '(a, b) <> (SELECT x, y FROM u)'];
        yield 'not equal bang' => ['(a, b) != (SELECT x, y FROM u)', '(a, b) <> (SELECT x, y FROM u)'];
        yield 'less than' => ['(a, b) < (SELECT x, y FROM u)', '(a, b) < (SELECT x, y FROM u)'];
        yield 'less than or equal' => ['(a, b) <= (SELECT x, y FROM u)', '(a, b) <= (SELECT x, y FROM u)'];
        yield 'greater than' => ['(a, b) > (SELECT x, y FROM u)', '(a, b) > (SELECT x, y FROM u)'];
        yield 'greater than or equal' => ['(a, b) >= (SELECT x, y FROM u)', '(a, b) >= (SELECT x, y FROM u)'];
        yield 'explicit row' => ['ROW(a, b) = (SELECT x, y FROM u)', 'ROW(a, b) = (SELECT x, y FROM u)'];
    }

    public static function top_level_nodes(): Generator
    {
        yield 'between symmetric' => ['b BETWEEN SYMMETRIC 1 AND 2'];
        yield 'not between symmetric' => ['b NOT BETWEEN SYMMETRIC 1 AND 2'];
        yield 'function call' => ['is_valid(x)'];
        yield 'type cast' => ['x::boolean'];
        yield 'scalar subquery' => ['(SELECT flag FROM flags LIMIT 1)'];
    }

    public function test_from_ast_names_unsupported_a_expr_kind(): void
    {
        $this->expectException(UnsupportedNodeException::class);
        $this->expectExceptionMessage('Unsupported node type: A_Expr kind: AEXPR_NULLIF');

        ConditionFactory::fromAst((new ExpressionParser())->parse('NULLIF(a, b)'));
    }

    public function test_from_ast_names_unsupported_bool_expr_type(): void
    {
        $this->expectException(UnsupportedNodeException::class);
        $this->expectExceptionMessage('Unsupported node type: BoolExpr type: BOOL_EXPR_TYPE_UNDEFINED');

        ConditionFactory::fromAst(new Node(['bool_expr' => new BoolExpr([
            'boolop' => BoolExprType::BOOL_EXPR_TYPE_UNDEFINED,
        ])]));
    }

    public function test_from_ast_names_unsupported_sub_link_type(): void
    {
        $this->expectException(UnsupportedNodeException::class);
        $this->expectExceptionMessage('Unsupported node type: SubLink type: ARRAY_SUBLINK');

        ConditionFactory::fromAst((new ExpressionParser())->parse('ARRAY(SELECT 1)'));
    }

    #[DataProvider('row_comparisons_with_subquery')]
    public function test_from_ast_round_trips_row_comparison_with_subquery(string $sql, string $expected): void
    {
        $condition = ConditionFactory::fromAst((new ExpressionParser())->parse($sql));

        static::assertInstanceOf(Comparison::class, $condition);
        static::assertSame(
            "SELECT 1 FROM t WHERE {$expected}",
            select(literal(1))->from(table('t'))->where($condition)->toSql(),
        );
    }

    #[DataProvider('top_level_nodes')]
    public function test_from_ast_round_trips_top_level_node(string $sql): void
    {
        static::assertSame(
            "SELECT 1 FROM t WHERE {$sql}",
            select(literal(1))
                ->from(table('t'))
                ->where(ConditionFactory::fromAst((new ExpressionParser())->parse($sql)))
                ->toSql(),
        );
    }
}
