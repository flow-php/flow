<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use PHPUnit\Framework\TestCase;

final class PrimaryKeyConstraintTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_composite_primary_key() : void
    {
        $constraint = PrimaryKeyConstraint::create('user_id', 'order_id');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        self::assertCount(2, $ast->getKeys());
        self::assertSame('user_id', $ast->getKeys()[0]->getString()->getSval());
        self::assertSame('order_id', $ast->getKeys()[1]->getString()->getSval());
    }

    public function test_immutability() : void
    {
        $original = PrimaryKeyConstraint::create('id');
        $withName = $original->name('pk_test');

        self::assertNotSame($original, $withName);
        self::assertSame('', $original->toAst()->getConname());
        self::assertSame('pk_test', $withName->toAst()->getConname());
    }

    public function test_primary_key_with_name() : void
    {
        $constraint = PrimaryKeyConstraint::create('id')
            ->name('pk_users');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        self::assertSame('pk_users', $ast->getConname());
        self::assertCount(1, $ast->getKeys());
        self::assertSame('id', $ast->getKeys()[0]->getString()->getSval());
    }

    public function test_simple_primary_key() : void
    {
        $constraint = PrimaryKeyConstraint::create('id');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getContype());
        self::assertCount(1, $ast->getKeys());
        self::assertSame('id', $ast->getKeys()[0]->getString()->getSval());
    }
}
