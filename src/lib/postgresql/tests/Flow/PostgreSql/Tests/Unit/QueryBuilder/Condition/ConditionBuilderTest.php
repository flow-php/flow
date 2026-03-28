<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use function Flow\PostgreSql\DSL\{col, conditions, eq, literal};
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\QueryBuilder\Condition\{AndCondition, ConditionBuilder, OrCondition};
use PHPUnit\Framework\TestCase;

final class ConditionBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_on_empty_builder_returns_new_builder_with_condition() : void
    {
        $cond = eq(col('x'), literal(1));

        $builder = ConditionBuilder::create();
        $result = $builder->and($cond);

        self::assertTrue($builder->isEmpty());
        self::assertFalse($result->isEmpty());
    }

    public function test_builder_is_immutable() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = ConditionBuilder::create();
        $modified = $original->and($cond1);
        $modified2 = $modified->and($cond2);

        self::assertTrue($original->isEmpty());
        self::assertNotSame($original, $modified);
        self::assertNotSame($modified, $modified2);
    }

    public function test_empty_builder_is_empty() : void
    {
        $builder = ConditionBuilder::create();

        self::assertTrue($builder->isEmpty());
        self::assertNull($builder->getCondition());
    }

    public function test_empty_nested_builder_is_ignored_in_and() : void
    {
        $cond = eq(col('x'), literal(1));
        $emptyBuilder = conditions();

        $builder = conditions()
            ->and($cond)
            ->and($emptyBuilder);

        self::assertFalse($builder->isEmpty());
        self::assertSame($cond, $builder->getCondition());
    }

    public function test_empty_nested_builder_is_ignored_in_or() : void
    {
        $cond = eq(col('x'), literal(1));
        $emptyBuilder = conditions();

        $builder = conditions()
            ->or($cond)
            ->or($emptyBuilder);

        self::assertFalse($builder->isEmpty());
        self::assertSame($cond, $builder->getCondition());
    }

    public function test_mixing_and_then_or() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()
            ->and($cond1)
            ->and($cond2)
            ->or($cond3);

        $condition = $builder->getCondition();
        self::assertInstanceOf(OrCondition::class, $condition);
    }

    public function test_mixing_or_then_and() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()
            ->or($cond1)
            ->or($cond2)
            ->and($cond3);

        $condition = $builder->getCondition();
        self::assertInstanceOf(AndCondition::class, $condition);
    }

    public function test_multiple_and_conditions() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()
            ->and($cond1)
            ->and($cond2)
            ->and($cond3);

        self::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        self::assertInstanceOf(AndCondition::class, $condition);

        $ast = $condition->toAst();
        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        self::assertCount(3, $boolExpr->getArgs());
    }

    public function test_multiple_or_conditions() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()
            ->or($cond1)
            ->or($cond2)
            ->or($cond3);

        self::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        self::assertInstanceOf(OrCondition::class, $condition);

        $ast = $condition->toAst();
        self::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        self::assertNotNull($boolExpr);
        self::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        self::assertCount(3, $boolExpr->getArgs());
    }

    public function test_nested_builder_via_and() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $nestedBuilder = conditions()
            ->or($cond2)
            ->or($cond3);

        $builder = conditions()
            ->and($cond1)
            ->and($nestedBuilder);

        self::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        self::assertInstanceOf(AndCondition::class, $condition);
    }

    public function test_nested_builder_via_or() : void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $nestedBuilder = conditions()
            ->and($cond2)
            ->and($cond3);

        $builder = conditions()
            ->or($cond1)
            ->or($nestedBuilder);

        self::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        self::assertInstanceOf(OrCondition::class, $condition);
    }

    public function test_or_on_empty_builder_returns_new_builder_with_condition() : void
    {
        $cond = eq(col('x'), literal(1));

        $builder = conditions();
        $result = $builder->or($cond);

        self::assertTrue($builder->isEmpty());
        self::assertFalse($result->isEmpty());
    }

    public function test_single_condition_via_and() : void
    {
        $cond = eq(col('x'), literal(1));
        $builder = conditions()->and($cond);

        self::assertFalse($builder->isEmpty());
        self::assertSame($cond, $builder->getCondition());
    }

    public function test_single_condition_via_or() : void
    {
        $cond = eq(col('x'), literal(1));
        $builder = conditions()->or($cond);

        self::assertFalse($builder->isEmpty());
        self::assertSame($cond, $builder->getCondition());
    }
}
