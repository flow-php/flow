<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\CheckConstraint;
use PHPUnit\Framework\TestCase;

final class CheckConstraintTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_check_constraint_with_name() : void
    {
        $constraint = CheckConstraint::create('age > 0')
            ->name('chk_positive_age');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        self::assertSame('chk_positive_age', $ast->getConname());
    }

    public function test_complex_check_expression() : void
    {
        $constraint = CheckConstraint::create('start_date < end_date');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        self::assertTrue($ast->hasRawExpr());
    }

    public function test_immutability() : void
    {
        $original = CheckConstraint::create('value > 0');
        $withName = $original->name('chk_test');

        self::assertNotSame($original, $withName);
        self::assertSame('', $original->toAst()->getConname());
        self::assertSame('chk_test', $withName->toAst()->getConname());
    }

    public function test_no_inherit() : void
    {
        $constraint = CheckConstraint::create('status IN (\'active\', \'inactive\')')
            ->noInherit();

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        self::assertTrue($ast->getIsNoInherit());
    }

    public function test_simple_check_constraint() : void
    {
        $constraint = CheckConstraint::create('age > 0');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getContype());
        self::assertTrue($ast->hasRawExpr());
    }

    public function test_with_all_options() : void
    {
        $constraint = CheckConstraint::create('amount >= 0')
            ->name('chk_positive_amount')
            ->noInherit();

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('chk_positive_amount', $ast->getConname());
        self::assertTrue($ast->getIsNoInherit());
    }
}
