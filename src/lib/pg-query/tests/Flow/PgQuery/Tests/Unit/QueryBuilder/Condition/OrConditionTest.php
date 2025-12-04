<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{BoolExpr, BoolExprType, Node};
use Flow\PgQuery\QueryBuilder\Condition\{AndCondition, NotCondition, OrCondition, RawCondition};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

final class OrConditionTest extends TestCase
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
        $cond3 = new RawCondition('z = 3');

        $or = new OrCondition($cond1, $cond2);
        $and = $or->and($cond3);

        self::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_empty_or_condition() : void
    {
        $or = new OrCondition();
        $ast = $or->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        self::assertCount(0, $boolExpr->getArgs());
    }

    public function test_from_ast_reconstructs_or_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $original = new OrCondition($cond1, $cond2);
        $ast = $original->toAst();

        $reconstructed = OrCondition::fromAst($ast);

        self::assertInstanceOf(OrCondition::class, $reconstructed);

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
        OrCondition::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_bool_type() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BoolExpr with OR_EXPR');

        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::AND_EXPR);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        OrCondition::fromAst($node);
    }

    public function test_not_returns_not_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $or = new OrCondition($cond1, $cond2);
        $not = $or->not();

        self::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_flattens_multiple_or_conditions() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');
        $cond3 = new RawCondition('z = 3');

        $or1 = new OrCondition($cond1, $cond2);
        $or2 = $or1->or($cond3);

        $ast = $or2->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        self::assertCount(3, $boolExpr->getArgs());
    }

    public function test_or_method_returns_new_instance() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $original = new OrCondition($cond1);
        $modified = $original->or($cond2);

        self::assertNotSame($original, $modified);
    }

    public function test_or_with_another_or_condition_flattens() : void
    {
        $cond1 = new RawCondition('a = 1');
        $cond2 = new RawCondition('b = 2');
        $cond3 = new RawCondition('c = 3');
        $cond4 = new RawCondition('d = 4');

        $or1 = new OrCondition($cond1, $cond2);
        $or2 = new OrCondition($cond3, $cond4);
        $combined = $or1->or($or2);

        $ast = $combined->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertCount(4, $boolExpr->getArgs());
    }

    public function test_or_with_single_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $or = new OrCondition($cond1, $cond2);
        $ast = $or->toAst();

        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        self::assertCount(2, $boolExpr->getArgs());
    }

    public function test_to_ast_creates_bool_expr() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $or = new OrCondition($cond1, $cond2);
        $ast = $or->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
    }
}
