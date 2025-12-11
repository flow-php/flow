<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use PHPUnit\Framework\TestCase;

final class ForeignKeyConstraintTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_composite_foreign_key() : void
    {
        $constraint = ForeignKeyConstraint::create(
            ['user_id', 'order_id'],
            'user_orders',
            ['user_id', 'order_id'],
        );

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        self::assertCount(2, $ast->getFkAttrs());
        self::assertCount(2, $ast->getPkAttrs());
    }

    public function test_deferrable() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->deferrable();

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertTrue($ast->getDeferrable());
        self::assertFalse($ast->getInitdeferred());
    }

    public function test_deferrable_initially_deferred() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->deferrable(initiallyDeferred: true);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertTrue($ast->getDeferrable());
        self::assertTrue($ast->getInitdeferred());
    }

    public function test_foreign_key_with_all_options() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->name('fk_orders_user')
            ->schema('public')
            ->onUpdate(ReferentialAction::CASCADE)
            ->onDelete(ReferentialAction::SET_NULL)
            ->deferrable(initiallyDeferred: true);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('fk_orders_user', $ast->getConname());
        self::assertSame('public', $ast->getPktable()->getSchemaname());
        self::assertSame('c', $ast->getFkUpdAction());
        self::assertSame('n', $ast->getFkDelAction());
        self::assertTrue($ast->getDeferrable());
        self::assertTrue($ast->getInitdeferred());
    }

    public function test_foreign_key_with_name() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->name('fk_orders_user');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('fk_orders_user', $ast->getConname());
    }

    public function test_foreign_key_with_schema() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->schema('public');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('public', $ast->getPktable()->getSchemaname());
        self::assertSame('users', $ast->getPktable()->getRelname());
    }

    public function test_immutability() : void
    {
        $original = ForeignKeyConstraint::create(['user_id'], 'users', ['id']);
        $withName = $original->name('fk_test');

        self::assertNotSame($original, $withName);
        self::assertSame('', $original->toAst()->getConname());
        self::assertSame('fk_test', $withName->toAst()->getConname());
    }

    public function test_on_delete_cascade() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onDelete(ReferentialAction::CASCADE);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('c', $ast->getFkDelAction());
    }

    public function test_on_delete_restrict() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onDelete(ReferentialAction::RESTRICT);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('r', $ast->getFkDelAction());
    }

    public function test_on_delete_set_default() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onDelete(ReferentialAction::SET_DEFAULT);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('d', $ast->getFkDelAction());
    }

    public function test_on_delete_set_null() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onDelete(ReferentialAction::SET_NULL);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('n', $ast->getFkDelAction());
    }

    public function test_on_update_cascade() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onUpdate(ReferentialAction::CASCADE);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('c', $ast->getFkUpdAction());
    }

    public function test_on_update_no_action() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->onUpdate(ReferentialAction::NO_ACTION);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame('a', $ast->getFkUpdAction());
    }

    public function test_simple_foreign_key() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id']);

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        self::assertCount(1, $ast->getFkAttrs());
        self::assertSame('user_id', $ast->getFkAttrs()[0]->getString()->getSval());
        self::assertSame('users', $ast->getPktable()->getRelname());
        self::assertCount(1, $ast->getPkAttrs());
        self::assertSame('id', $ast->getPkAttrs()[0]->getString()->getSval());
    }

    public function test_simple_foreign_key_without_reference_columns() : void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users');

        $ast = $constraint->toAst();

        self::assertInstanceOf(Constraint::class, $ast);
        self::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        self::assertSame('users', $ast->getPktable()->getRelname());
        self::assertCount(0, $ast->getPkAttrs());
    }
}
