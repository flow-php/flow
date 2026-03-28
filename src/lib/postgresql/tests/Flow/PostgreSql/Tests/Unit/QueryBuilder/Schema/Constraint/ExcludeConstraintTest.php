<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use function Flow\PostgreSql\DSL\{col, eq, literal};
use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint;
use PHPUnit\Framework\TestCase;

final class ExcludeConstraintTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_exclude_constraint_with_btree() : void
    {
        $constraint = ExcludeConstraint::create('btree')
            ->element(col('id'), '=');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertSame('btree', $ast->getAccessMethod());
    }

    public function test_exclude_constraint_with_multiple_elements() : void
    {
        $constraint = ExcludeConstraint::create('gist')
            ->element(col('room_id'), '=')
            ->element(col('during'), '&&');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertCount(4, $ast->getExclusions());
    }

    public function test_exclude_constraint_with_name() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=')
            ->name('exc_room_booking');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('exc_room_booking', $ast->getConname());
    }

    public function test_exclude_constraint_with_where_clause() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=')
            ->where(eq(col('active'), literal(true)));

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertTrue($ast->hasWhereClause());
    }

    public function test_exclude_with_all_options() : void
    {
        $constraint = ExcludeConstraint::create('gist')
            ->name('exc_booking')
            ->element(col('room_id'), '=')
            ->element(col('period'), '&&')
            ->where(eq(col('cancelled'), literal(false)));

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('exc_booking', $ast->getConname());
        self::assertSame('gist', $ast->getAccessMethod());
        self::assertCount(4, $ast->getExclusions());
        self::assertTrue($ast->hasWhereClause());
    }

    public function test_immutability() : void
    {
        $original = ExcludeConstraint::create();
        $withName = $original->name('exc_test');

        self::assertNotSame($original, $withName);
        self::assertSame('', $original->toAst()->getConname());
        self::assertSame('exc_test', $withName->toAst()->getConname());
    }

    public function test_simple_exclude_constraint() : void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        self::assertSame('gist', $ast->getAccessMethod());
        self::assertCount(2, $ast->getExclusions());
    }
}
