<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class OrConditionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public static function child_nodes(): Generator
    {
        yield 'null test' => ['x IS NULL'];
        yield 'not null test' => ['x IS NOT NULL'];
        yield 'in' => ['x IN (1, 2)'];
        yield 'bare boolean column' => ['active'];
        yield 'boolean literal' => ['true'];
        yield 'between symmetric' => ['x BETWEEN SYMMETRIC 1 AND 2'];
        yield 'non comparison operator' => ['tags @> ARRAY[\'x\']'];
        yield 'function call' => ['is_valid(x)'];
        yield 'scalar subquery' => ['(SELECT flag FROM flags LIMIT 1)'];
        yield 'row comparison with subquery' => ['(a, b) = (SELECT x, y FROM u)'];
    }

    #[DataProvider('child_nodes')]
    public function test_from_ast_round_trips_child_node(string $child): void
    {
        static::assertSame(
            "SELECT 1 FROM t WHERE a = 1 OR {$child}",
            select(literal(1))
                ->from(table('t'))
                ->where(OrCondition::fromAst((new ExpressionParser())->parse("a = 1 OR {$child}")))
                ->toSql(),
        );
    }

    public function test_from_ast_round_trips_two_null_tests(): void
    {
        static::assertSame(
            'SELECT 1 FROM t WHERE failed_at IS NULL OR finished_at IS NULL',
            select(literal(1))
                ->from(table('t'))
                ->where(OrCondition::fromAst((new ExpressionParser())->parse(
                    'failed_at IS NULL OR finished_at IS NULL',
                )))
                ->toSql(),
        );
    }

    public function test_and_returns_and_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $or = new OrCondition($cond1, $cond2);
        $and = $or->and($cond3);

        static::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_empty_or_condition(): void
    {
        $or = new OrCondition();
        $ast = $or->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        static::assertCount(0, $boolExpr->getArgs());
    }

    public function test_from_ast_reconstructs_or_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = new OrCondition($cond1, $cond2);
        $ast = $original->toAst();

        $reconstructed = OrCondition::fromAst($ast);

        static::assertInstanceOf(OrCondition::class, $reconstructed);

        $reconstructedAst = $reconstructed->toAst();
        static::assertTrue($reconstructedAst->hasBoolExpr());

        $boolExpr = $reconstructedAst->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertCount(2, $boolExpr->getArgs());
    }

    public function test_from_ast_throws_on_non_bool_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr node, got unknown');

        $node = new Node();
        OrCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with OR_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::AND_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        OrCondition::fromAst($node);
    }

    public function test_not_returns_not_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $or = new OrCondition($cond1, $cond2);
        $not = $or->not();

        static::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_flattens_multiple_or_conditions(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $or1 = new OrCondition($cond1, $cond2);
        $or2 = $or1->or($cond3);

        $ast = $or2->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        static::assertCount(3, $boolExpr->getArgs());
    }

    public function test_or_method_returns_new_instance(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = new OrCondition($cond1);
        $modified = $original->or($cond2);

        static::assertNotSame($original, $modified);
    }

    public function test_or_with_another_or_condition_flattens(): void
    {
        $cond1 = eq(col('a'), literal(1));
        $cond2 = eq(col('b'), literal(2));
        $cond3 = eq(col('c'), literal(3));
        $cond4 = eq(col('d'), literal(4));

        $or1 = new OrCondition($cond1, $cond2);
        $or2 = new OrCondition($cond3, $cond4);
        $combined = $or1->or($or2);

        $ast = $combined->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertCount(4, $boolExpr->getArgs());
    }

    public function test_or_with_single_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $or = new OrCondition($cond1, $cond2);
        $ast = $or->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        static::assertCount(2, $boolExpr->getArgs());
    }

    public function test_to_ast_creates_bool_expr(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $or = new OrCondition($cond1, $cond2);
        $ast = $or->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
    }
}
