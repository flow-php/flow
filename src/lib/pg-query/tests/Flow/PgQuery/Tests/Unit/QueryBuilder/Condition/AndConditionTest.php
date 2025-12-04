<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{BoolExpr, BoolExprType, Node};
use Flow\PgQuery\QueryBuilder\Condition\{AndCondition, NotCondition, OrCondition, RawCondition};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

final class AndConditionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_flattens_multiple_and_conditions() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');
        $cond3 = new RawCondition('z = 3');

        $and1 = new AndCondition($cond1, $cond2);
        $and2 = $and1->and($cond3);

        $ast = $and2->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        self::assertCount(3, $boolExpr->getArgs());
    }

    public function test_and_method_returns_new_instance() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $original = new AndCondition($cond1);
        $modified = $original->and($cond2);

        self::assertNotSame($original, $modified);
    }

    public function test_and_with_another_and_condition_flattens() : void
    {
        $cond1 = new RawCondition('a = 1');
        $cond2 = new RawCondition('b = 2');
        $cond3 = new RawCondition('c = 3');
        $cond4 = new RawCondition('d = 4');

        $and1 = new AndCondition($cond1, $cond2);
        $and2 = new AndCondition($cond3, $cond4);
        $combined = $and1->and($and2);

        $ast = $combined->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertCount(4, $boolExpr->getArgs());
    }

    public function test_and_with_single_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $and = new AndCondition($cond1, $cond2);
        $ast = $and->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        self::assertCount(2, $boolExpr->getArgs());
    }

    public function test_empty_and_condition() : void
    {
        $and = new AndCondition();
        $ast = $and->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        self::assertCount(0, $boolExpr->getArgs());
    }

    public function test_from_ast_reconstructs_and_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $original = new AndCondition($cond1, $cond2);
        $ast = $original->toAst();

        $reconstructed = AndCondition::fromAst($ast);

        self::assertInstanceOf(AndCondition::class, $reconstructed);

        $reconstructedAst = $reconstructed->toAst();
        self::assertTrue($reconstructedAst->hasBoolExpr());

        $boolExpr = $reconstructedAst->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertCount(2, $boolExpr->getArgs());
    }

    public function test_from_ast_throws_on_non_bool_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr node, got unknown');

        $node = new Node();
        AndCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with AND_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::OR_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        AndCondition::fromAst($node);
    }

    public function test_not_returns_not_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $and = new AndCondition($cond1, $cond2);
        $not = $and->not();

        self::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');
        $cond3 = new RawCondition('z = 3');

        $and = new AndCondition($cond1, $cond2);
        $or = $and->or($cond3);

        self::assertInstanceOf(OrCondition::class, $or);
    }

    public function test_to_ast_creates_bool_expr() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $and = new AndCondition($cond1, $cond2);
        $ast = $and->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
    }
}
