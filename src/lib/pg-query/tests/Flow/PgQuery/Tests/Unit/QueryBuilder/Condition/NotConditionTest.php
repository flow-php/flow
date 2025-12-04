<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{BoolExpr, BoolExprType, Node};
use Flow\PgQuery\QueryBuilder\Condition\{AndCondition, NotCondition, OrCondition, RawCondition};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

final class NotConditionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_returns_and_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $not = new NotCondition($cond1);
        $and = $not->and($cond2);

        self::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_double_negation() : void
    {
        $cond = new RawCondition('x = 1');
        $not1 = new NotCondition($cond);
        $not2 = $not1->not();

        $ast = $not2->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        self::assertCount(1, $boolExpr->getArgs());

        $innerNode = $boolExpr->getArgs()[0];
        self::assertTrue($innerNode->hasBoolExpr());

        $innerBoolExpr = $innerNode->getBoolExpr();
        self::assertNotNull($innerBoolExpr);
        self::assertSame(BoolExprType::NOT_EXPR, $innerBoolExpr->getBoolop());
    }

    public function test_from_ast_reconstructs_not_condition() : void
    {
        $cond = new RawCondition('x = 1');
        $original = new NotCondition($cond);
        $ast = $original->toAst();

        $reconstructed = NotCondition::fromAst($ast);

        self::assertInstanceOf(NotCondition::class, $reconstructed);

        $reconstructedAst = $reconstructed->toAst();
        self::assertTrue($reconstructedAst->hasBoolExpr());

        $boolExpr = $reconstructedAst->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        self::assertCount(1, $boolExpr->getArgs());
    }

    public function test_from_ast_throws_on_empty_args() : void
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

    public function test_from_ast_throws_on_multiple_args() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('NOT_EXPR must have exactly one argument, got 2');

        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::NOT_EXPR);
        $boolExpr->setArgs([$cond1->toAst(), $cond2->toAst()]);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        NotCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_non_bool_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr node, got unknown');

        $node = new Node();
        NotCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with NOT_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::AND_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        NotCondition::fromAst($node);
    }

    public function test_not_negates_condition() : void
    {
        $cond = new RawCondition('x = 1');
        $not = new NotCondition($cond);
        $ast = $not->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
        self::assertCount(1, $boolExpr->getArgs());
    }

    public function test_not_returns_not_condition() : void
    {
        $cond = new RawCondition('x = 1');
        $not = new NotCondition($cond);
        $notNot = $not->not();

        self::assertInstanceOf(NotCondition::class, $notNot);
    }

    public function test_or_returns_or_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $not = new NotCondition($cond1);
        $or = $not->or($cond2);

        self::assertInstanceOf(OrCondition::class, $or);
    }

    public function test_to_ast_creates_bool_expr() : void
    {
        $cond = new RawCondition('x = 1');
        $not = new NotCondition($cond);
        $ast = $not->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::NOT_EXPR, $boolExpr->getBoolop());
    }
}
