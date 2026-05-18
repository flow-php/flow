<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Types\DSL\type_instance_of;

final class ForeignKeyConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
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
        $pktable = $ast->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('public', $pktable->getSchemaname());
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
        $pktable = $ast->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('public', $pktable->getSchemaname());
        static::assertSame('users', $pktable->getRelname());
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
        $fkAttrs = $ast->getFkAttrs();
        static::assertCount(1, $fkAttrs);
        $fkString = type_instance_of(Node::class)->assert($fkAttrs[0])->getString();
        static::assertNotNull($fkString);
        static::assertSame('user_id', $fkString->getSval());
        $pktable = $ast->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('users', $pktable->getRelname());
        $pkAttrs = $ast->getPkAttrs();
        static::assertCount(1, $pkAttrs);
        $pkString = type_instance_of(Node::class)->assert($pkAttrs[0])->getString();
        static::assertNotNull($pkString);
        static::assertSame('id', $pkString->getSval());
    }

    public function test_simple_foreign_key_without_reference_columns(): void
    {
        $constraint = ForeignKeyConstraint::create(['user_id'], 'users');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_FOREIGN, $ast->getContype());
        $pktable = $ast->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('users', $pktable->getRelname());
        static::assertCount(0, $ast->getPkAttrs());
    }
}
