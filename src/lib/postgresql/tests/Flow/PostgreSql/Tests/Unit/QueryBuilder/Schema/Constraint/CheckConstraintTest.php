<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\CheckConstraint;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\ge;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\in_;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\lt;

final class CheckConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_check_constraint_with_name(): void
    {
        $constraint = CheckConstraint::create(gt(col('age'), literal(0)))->name('chk_positive_age');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        static::assertSame('chk_positive_age', $ast->getConname());
    }

    public function test_complex_check_expression(): void
    {
        $constraint = CheckConstraint::create(lt(col('start_date'), col('end_date')));

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        static::assertTrue($ast->hasRawExpr());
    }

    public function test_immutability(): void
    {
        $original = CheckConstraint::create(gt(col('value'), literal(0)));
        $withName = $original->name('chk_test');

        static::assertNotSame($original, $withName);
        static::assertSame('', $original->toAst()->getConname());
        static::assertSame('chk_test', $withName->toAst()->getConname());
    }

    public function test_no_inherit(): void
    {
        $constraint = CheckConstraint::create(in_(col('status'), [
            literal('active'),
            literal('inactive'),
        ]))->noInherit();

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        static::assertTrue($ast->getIsNoInherit());
    }

    public function test_simple_check_constraint(): void
    {
        $constraint = CheckConstraint::create(gt(col('age'), literal(0)));

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        static::assertTrue($ast->hasRawExpr());
    }

    public function test_with_all_options(): void
    {
        $constraint = CheckConstraint::create(ge(col('amount'), literal(0)))
            ->name('chk_positive_amount')
            ->noInherit();

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('chk_positive_amount', $ast->getConname());
        static::assertTrue($ast->getIsNoInherit());
    }
}
