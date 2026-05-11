<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use PHPUnit\Framework\TestCase;

final class ForeignKeyConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_composite_foreign_key(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id', 'order_id'], 'user_orders', ['user_id', 'order_id']);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        static::assertCount(2, $ast->getFkAttrs());
        static::assertCount(2, $ast->getPkAttrs());
    }

    public function test_deferrable(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->deferrable();

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertTrue($ast->getDeferrable());
        static::assertFalse($ast->getInitdeferred());
    }

    public function test_deferrable_initially_deferred(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->deferrable(initiallyDeferred: true);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertTrue($ast->getDeferrable());
        static::assertTrue($ast->getInitdeferred());
    }

    public function test_foreign_key_with_all_options(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])
            ->name('fk_orders_user')
            ->schema('public')
            ->onUpdate(ReferentialAction::CASCADE)
            ->onDelete(ReferentialAction::SET_NULL)
            ->deferrable(initiallyDeferred: true);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('fk_orders_user', $ast->getConname());
        static::assertSame('public', $ast->getPktable()->getSchemaname());
        static::assertSame('c', $ast->getFkUpdAction());
        static::assertSame('n', $ast->getFkDelAction());
        static::assertTrue($ast->getDeferrable());
        static::assertTrue($ast->getInitdeferred());
    }

    public function test_foreign_key_with_name(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->name('fk_orders_user');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('fk_orders_user', $ast->getConname());
    }

    public function test_foreign_key_with_schema(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->schema('public');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('public', $ast->getPktable()->getSchemaname());
        static::assertSame('users', $ast->getPktable()->getRelname());
    }

    public function test_immutability(): void
    {
        $original = ForeignKeyConstraint::create(['user_id'], 'users', ['id']);
        $withName = $original->name('fk_test');

        static::assertNotSame($original, $withName);
        static::assertSame('', $original->toAst()->getConname());
        static::assertSame('fk_test', $withName->toAst()->getConname());
    }

    public function test_on_delete_cascade(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->onDelete(ReferentialAction::CASCADE);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('c', $ast->getFkDelAction());
    }

    public function test_on_delete_restrict(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->onDelete(ReferentialAction::RESTRICT);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('r', $ast->getFkDelAction());
    }

    public function test_on_delete_set_default(): void
    {
        $constraint = ForeignKeyConstraint::create(
            ['user_id'],
            'users',
            ['id'],
        )->onDelete(ReferentialAction::SET_DEFAULT);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('d', $ast->getFkDelAction());
    }

    public function test_on_delete_set_null(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->onDelete(ReferentialAction::SET_NULL);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('n', $ast->getFkDelAction());
    }

    public function test_on_update_cascade(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id'])->onUpdate(ReferentialAction::CASCADE);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('c', $ast->getFkUpdAction());
    }

    public function test_on_update_no_action(): void
    {
        $constraint = ForeignKeyConstraint::create(
            ['user_id'],
            'users',
            ['id'],
        )->onUpdate(ReferentialAction::NO_ACTION);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('a', $ast->getFkUpdAction());
    }

    public function test_simple_foreign_key(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users', ['id']);

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        static::assertCount(1, $ast->getFkAttrs());
        static::assertSame('user_id', $ast->getFkAttrs()[0]->getString()->getSval());
        static::assertSame('users', $ast->getPktable()->getRelname());
        static::assertCount(1, $ast->getPkAttrs());
        static::assertSame('id', $ast->getPkAttrs()[0]->getString()->getSval());
    }

    public function test_simple_foreign_key_without_reference_columns(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        static::assertSame('users', $ast->getPktable()->getRelname());
        static::assertCount(0, $ast->getPkAttrs());
    }
}
