<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;

final class AndConditionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_and_flattens_multiple_and_conditions(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $and1 = new AndCondition($cond1, $cond2);
        $and2 = $and1->and($cond3);

        $ast = $and2->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        static::assertCount(3, $boolExpr->getArgs());
    }

    public function test_and_method_returns_new_instance(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = new AndCondition($cond1);
        $modified = $original->and($cond2);

        static::assertNotSame($original, $modified);
    }

    public function test_and_with_another_and_condition_flattens(): void
    {
        $cond1 = eq(col('a'), literal(1));
        $cond2 = eq(col('b'), literal(2));
        $cond3 = eq(col('c'), literal(3));
        $cond4 = eq(col('d'), literal(4));

        $and1 = new AndCondition($cond1, $cond2);
        $and2 = new AndCondition($cond3, $cond4);
        $combined = $and1->and($and2);

        $ast = $combined->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertCount(4, $boolExpr->getArgs());
    }

    public function test_and_with_single_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $and = new AndCondition($cond1, $cond2);
        $ast = $and->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        static::assertCount(2, $boolExpr->getArgs());
    }

    public function test_empty_and_condition(): void
    {
        $and = new AndCondition();
        $ast = $and->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        static::assertCount(0, $boolExpr->getArgs());
    }

    public function test_from_ast_reconstructs_and_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = new AndCondition($cond1, $cond2);
        $ast = $original->toAst();

        $reconstructed = AndCondition::fromAst($ast);

        static::assertInstanceOf(AndCondition::class, $reconstructed);

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
        AndCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with AND_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::OR_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        AndCondition::fromAst($node);
    }

    public function test_not_returns_not_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $and = new AndCondition($cond1, $cond2);
        $not = $and->not();

        static::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $and = new AndCondition($cond1, $cond2);
        $or = $and->or($cond3);

        static::assertInstanceOf(OrCondition::class, $or);
    }

    public function test_to_ast_creates_bool_expr(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $and = new AndCondition($cond1, $cond2);
        $ast = $and->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
    }
}
