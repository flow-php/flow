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

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;

final class NotConditionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_and_returns_and_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $not = new NotCondition($cond1);
        $and = $not->and($cond2);

        static::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_double_negation(): void
    {
        $cond = eq(col('x'), literal(1));
        $not1 = new NotCondition($cond);
        $not2 = $not1->not();

        $ast = $not2->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        static::assertCount(1, $boolExpr->getArgs());

        $innerNode = $boolExpr->getArgs()[0];
        static::assertTrue($innerNode->hasBoolExpr());

        $innerBoolExpr = $innerNode->getBoolExpr();
        static::assertNotNull($innerBoolExpr);
        static::assertSame(BoolExprType::NOT_EXPR, $innerBoolExpr->getBoolop());
    }

    public function test_from_ast_reconstructs_not_condition(): void
    {
        $cond = eq(col('x'), literal(1));
        $original = new NotCondition($cond);
        $ast = $original->toAst();

        $reconstructed = NotCondition::fromAst($ast);

        static::assertInstanceOf(NotCondition::class, $reconstructed);

        $reconstructedAst = $reconstructed->toAst();
        static::assertTrue($reconstructedAst->hasBoolExpr());

        $boolExpr = $reconstructedAst->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        static::assertCount(1, $boolExpr->getArgs());
    }

    public function test_from_ast_throws_on_empty_args(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('NOT_EXPR must have exactly one argument, got 0');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::NOT_EXPR);
        $boolExpr->setArgs([]);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        NotCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_multiple_args(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('NOT_EXPR must have exactly one argument, got 2');

        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::NOT_EXPR);
        $boolExpr->setArgs([$cond1->toAst(), $cond2->toAst()]);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        NotCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_non_bool_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr node, got unknown');

        $node = new Node();
        NotCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with NOT_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::AND_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        NotCondition::fromAst($node);
    }

    public function test_not_negates_condition(): void
    {
        $cond = eq(col('x'), literal(1));
        $not = new NotCondition($cond);
        $ast = $not->toAst();

        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        static::assertCount(1, $boolExpr->getArgs());
    }

    public function test_not_returns_not_condition(): void
    {
        $cond = eq(col('x'), literal(1));
        $not = new NotCondition($cond);
        $notNot = $not->not();

        static::assertInstanceOf(NotCondition::class, $notNot);
    }

    public function test_or_returns_or_condition(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $not = new NotCondition($cond1);
        $or = $not->or($cond2);

        static::assertInstanceOf(OrCondition::class, $or);
    }

    public function test_to_ast_creates_bool_expr(): void
    {
        $cond = eq(col('x'), literal(1));
        $not = new NotCondition($cond);
        $ast = $not->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
    }
}
