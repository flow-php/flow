<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use PHPUnit\Framework\TestCase;

final class PrimaryKeyConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_composite_primary_key(): void
    {
        $constraint = PrimaryKeyConstraint::create('user_id', 'order_id');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        static::assertCount(2, $ast->getKeys());
        static::assertSame('user_id', $ast->getKeys()[0]->getString()->getSval());
        static::assertSame('order_id', $ast->getKeys()[1]->getString()->getSval());
    }

    public function test_immutability(): void
    {
        $original = PrimaryKeyConstraint::create('id');
        $withName = $original->name('pk_test');

        static::assertNotSame($original, $withName);
        static::assertSame('', $original->toAst()->getConname());
        static::assertSame('pk_test', $withName->toAst()->getConname());
    }

    public function test_primary_key_with_name(): void
    {
        $constraint = PrimaryKeyConstraint::create('id')->name('pk_users');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        static::assertSame('pk_users', $ast->getConname());
        static::assertCount(1, $ast->getKeys());
        static::assertSame('id', $ast->getKeys()[0]->getString()->getSval());
    }

    public function test_simple_primary_key(): void
    {
        $constraint = PrimaryKeyConstraint::create('id');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        static::assertCount(1, $ast->getKeys());
        static::assertSame('id', $ast->getKeys()[0]->getString()->getSval());
    }
}
