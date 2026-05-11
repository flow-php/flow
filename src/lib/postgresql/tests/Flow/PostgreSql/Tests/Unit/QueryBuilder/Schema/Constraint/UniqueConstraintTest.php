<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint;
use PHPUnit\Framework\TestCase;

final class UniqueConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_composite_unique_constraint(): void
    {
        $constraint = UniqueConstraint::create('first_name', 'last_name');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getContype());
        static::assertCount(2, $ast->getKeys());
        static::assertSame('first_name', $ast->getKeys()[0]->getString()->getSval());
        static::assertSame('last_name', $ast->getKeys()[1]->getString()->getSval());
    }

    public function test_immutability(): void
    {
        $original = UniqueConstraint::create('email');
        $withName = $original->name('uq_test');

        static::assertNotSame($original, $withName);
        static::assertSame('', $original->toAst()->getConname());
        static::assertSame('uq_test', $withName->toAst()->getConname());
    }

    public function test_nulls_not_distinct(): void
    {
        $constraint = UniqueConstraint::create('email')->nullsNotDistinct();

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getContype());
        static::assertTrue($ast->getNullsNotDistinct());
    }

    public function test_simple_unique_constraint(): void
    {
        $constraint = UniqueConstraint::create('email');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getContype());
        static::assertCount(1, $ast->getKeys());
        static::assertSame('email', $ast->getKeys()[0]->getString()->getSval());
    }

    public function test_unique_constraint_with_name(): void
    {
        $constraint = UniqueConstraint::create('email')->name('uq_users_email');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getContype());
        static::assertSame('uq_users_email', $ast->getConname());
    }

    public function test_unique_with_all_options(): void
    {
        $constraint = UniqueConstraint::create('email')->name('uq_users_email')->nullsNotDistinct();

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('uq_users_email', $ast->getConname());
        static::assertTrue($ast->getNullsNotDistinct());
    }
}
