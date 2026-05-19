<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionBuilder;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\conditions;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;

final class ConditionBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_and_on_empty_builder_returns_new_builder_with_condition(): void
    {
        $cond = eq(col('x'), literal(1));

        $builder = ConditionBuilder::create();
        $result = $builder->and($cond);

        static::assertTrue($builder->isEmpty());
        static::assertFalse($result->isEmpty());
    }

    public function test_builder_is_immutable(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));

        $original = ConditionBuilder::create();
        $modified = $original->and($cond1);
        $modified2 = $modified->and($cond2);

        static::assertTrue($original->isEmpty());
        static::assertNotSame($original, $modified);
        static::assertNotSame($modified, $modified2);
    }

    public function test_empty_builder_is_empty(): void
    {
        $builder = ConditionBuilder::create();

        static::assertTrue($builder->isEmpty());
        static::assertNull($builder->getCondition());
    }

    public function test_empty_nested_builder_is_ignored_in_and(): void
    {
        $cond = eq(col('x'), literal(1));
        $emptyBuilder = conditions();

        $builder = conditions()->and($cond)->and($emptyBuilder);

        static::assertFalse($builder->isEmpty());
        static::assertSame($cond, $builder->getCondition());
    }

    public function test_empty_nested_builder_is_ignored_in_or(): void
    {
        $cond = eq(col('x'), literal(1));
        $emptyBuilder = conditions();

        $builder = conditions()->or($cond)->or($emptyBuilder);

        static::assertFalse($builder->isEmpty());
        static::assertSame($cond, $builder->getCondition());
    }

    public function test_mixing_and_then_or(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()->and($cond1)->and($cond2)->or($cond3);

        $condition = $builder->getCondition();
        static::assertInstanceOf(OrCondition::class, $condition);
    }

    public function test_mixing_or_then_and(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()->or($cond1)->or($cond2)->and($cond3);

        $condition = $builder->getCondition();
        static::assertInstanceOf(AndCondition::class, $condition);
    }

    public function test_multiple_and_conditions(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()->and($cond1)->and($cond2)->and($cond3);

        static::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        static::assertInstanceOf(AndCondition::class, $condition);

        $ast = $condition->toAst();
        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::AND_EXPR, $boolExpr->getBoolop());
        static::assertCount(3, $boolExpr->getArgs());
    }

    public function test_multiple_or_conditions(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $builder = conditions()->or($cond1)->or($cond2)->or($cond3);

        static::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        static::assertInstanceOf(OrCondition::class, $condition);

        $ast = $condition->toAst();
        static::assertTrue($ast->hasBoolExpr());

        $boolExpr = $ast->getBoolExpr();
        static::assertNotNull($boolExpr);
        static::assertSame(BoolExprType::OR_EXPR, $boolExpr->getBoolop());
        static::assertCount(3, $boolExpr->getArgs());
    }

    public function test_nested_builder_via_and(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $nestedBuilder = conditions()->or($cond2)->or($cond3);

        $builder = conditions()->and($cond1)->and($nestedBuilder);

        static::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        static::assertInstanceOf(AndCondition::class, $condition);
    }

    public function test_nested_builder_via_or(): void
    {
        $cond1 = eq(col('x'), literal(1));
        $cond2 = eq(col('y'), literal(2));
        $cond3 = eq(col('z'), literal(3));

        $nestedBuilder = conditions()->and($cond2)->and($cond3);

        $builder = conditions()->or($cond1)->or($nestedBuilder);

        static::assertFalse($builder->isEmpty());

        $condition = $builder->getCondition();
        static::assertInstanceOf(OrCondition::class, $condition);
    }

    public function test_or_on_empty_builder_returns_new_builder_with_condition(): void
    {
        $cond = eq(col('x'), literal(1));

        $builder = conditions();
        $result = $builder->or($cond);

        static::assertTrue($builder->isEmpty());
        static::assertFalse($result->isEmpty());
    }

    public function test_single_condition_via_and(): void
    {
        $cond = eq(col('x'), literal(1));
        $builder = conditions()->and($cond);

        static::assertFalse($builder->isEmpty());
        static::assertSame($cond, $builder->getCondition());
    }

    public function test_single_condition_via_or(): void
    {
        $cond = eq(col('x'), literal(1));
        $builder = conditions()->or($cond);

        static::assertFalse($builder->isEmpty());
        static::assertSame($cond, $builder->getCondition());
    }
}
