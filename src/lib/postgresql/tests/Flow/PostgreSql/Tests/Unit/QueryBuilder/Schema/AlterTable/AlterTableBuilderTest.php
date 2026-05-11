<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\AlterTable;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableType;
use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\AlterTable\AlterTableBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\AlterTable\RenameTableBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\QueryBuilder\Sql;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\literal;

final class AlterTableBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_add_column(): void
    {
        $builder = AlterTableBuilder::create('users')->addColumn(
            ColumnDefinition::create('email', ColumnType::varchar(255))->notNull(),
        );

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AddColumn, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->hasDef());
    }

    public function test_add_constraint(): void
    {
        $builder = AlterTableBuilder::create('users')->addConstraint(PrimaryKeyConstraint::create('id'));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AddConstraint, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
    }

    public function test_add_foreign_key_constraint(): void
    {
        $builder = AlterTableBuilder::create('orders')->addConstraint(ForeignKeyConstraint::create(
            ['user_id'],
            'users',
            ['id'],
        ));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AddConstraint, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
    }

    public function test_add_inherit(): void
    {
        $builder = AlterTableBuilder::create('employees')->addInherit('persons');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AddInherit, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->hasDef());
        static::assertSame('persons', $ast->getCmds()[0]->getAlterTableCmd()->getDef()->getRangeVar()->getRelname());
    }

    public function test_add_inherit_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE employees INHERIT persons',
            AlterTableBuilder::create('employees')->addInherit('persons')->toSql(),
        );
    }

    public function test_add_unique_constraint(): void
    {
        $builder = AlterTableBuilder::create('users')->addConstraint(UniqueConstraint::create('email'));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AddConstraint, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
    }

    public function test_alter_column_drop_default(): void
    {
        $builder = AlterTableBuilder::create('users')->alterColumnDropDefault('status');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_ColumnDefault, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('status', $ast->getCmds()[0]->getAlterTableCmd()->getName());
        static::assertFalse($ast->getCmds()[0]->getAlterTableCmd()->hasDef());
    }

    public function test_alter_column_drop_not_null(): void
    {
        $builder = AlterTableBuilder::create('users')->alterColumnDropNotNull('email');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_DropNotNull, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('email', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_alter_column_set_default(): void
    {
        $builder = AlterTableBuilder::create('users')->alterColumnSetDefault('status', literal('active'));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_ColumnDefault, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('status', $ast->getCmds()[0]->getAlterTableCmd()->getName());
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->hasDef());
    }

    public function test_alter_column_set_not_null(): void
    {
        $builder = AlterTableBuilder::create('users')->alterColumnSetNotNull('email');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetNotNull, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('email', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_alter_column_type(): void
    {
        $builder = AlterTableBuilder::create('users')->alterColumnType('name', ColumnType::text());

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_AlterColumnType, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('name', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_alter_table_if_exists(): void
    {
        $builder = AlterTableBuilder::create('users')
            ->ifExists()
            ->addColumn(ColumnDefinition::create('email', ColumnType::varchar(255)));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_table_with_schema(): void
    {
        $builder = AlterTableBuilder::create('users', 'public')->addColumn(ColumnDefinition::create(
            'email',
            ColumnType::varchar(255),
        ));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame('public', $ast->getRelation()->getSchemaname());
        static::assertSame('users', $ast->getRelation()->getRelname());
    }

    public function test_drop_column(): void
    {
        $builder = AlterTableBuilder::create('users')->dropColumn('temp_column');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_DropColumn, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('temp_column', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_drop_column_cascade(): void
    {
        $builder = AlterTableBuilder::create('users')->dropColumn('temp_column', cascade: true);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getCmds()[0]->getAlterTableCmd()->getBehavior());
    }

    public function test_drop_column_if_exists(): void
    {
        $builder = AlterTableBuilder::create('users')->dropColumnIfExists('temp_column');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_DropColumn, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->getMissingOk());
    }

    public function test_drop_constraint(): void
    {
        $builder = AlterTableBuilder::create('users')->dropConstraint('users_email_key');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_DropConstraint, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('users_email_key', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_drop_constraint_cascade(): void
    {
        $builder = AlterTableBuilder::create('users')->dropConstraint('users_email_key', cascade: true);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getCmds()[0]->getAlterTableCmd()->getBehavior());
    }

    public function test_drop_constraint_if_exists(): void
    {
        $builder = AlterTableBuilder::create('users')->dropConstraintIfExists('users_email_key');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->getMissingOk());
    }

    public function test_drop_inherit(): void
    {
        $builder = AlterTableBuilder::create('employees')->dropInherit('persons');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_DropInherit, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertTrue($ast->getCmds()[0]->getAlterTableCmd()->hasDef());
        static::assertSame('persons', $ast->getCmds()[0]->getAlterTableCmd()->getDef()->getRangeVar()->getRelname());
    }

    public function test_drop_inherit_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE employees NO INHERIT persons',
            AlterTableBuilder::create('employees')->dropInherit('persons')->toSql(),
        );
    }

    public function test_immutability(): void
    {
        $original = AlterTableBuilder::create('users');
        $modified = $original->addColumn(ColumnDefinition::create('email', ColumnType::varchar(255)));

        static::assertCount(0, $original->toAst()->getCmds());
        static::assertCount(1, $modified->toAst()->getCmds());
    }

    public function test_multiple_commands(): void
    {
        $builder = AlterTableBuilder::create('users')
            ->addColumn(ColumnDefinition::create('email', ColumnType::varchar(255))->notNull())
            ->addColumn(ColumnDefinition::create('phone', ColumnType::varchar(20)))
            ->addConstraint(UniqueConstraint::create('email'))
            ->alterColumnSetNotNull('phone');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(4, $ast->getCmds());
    }

    public function test_rename_column_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE users RENAME COLUMN name TO full_name',
            AlterTableBuilder::create('users')->renameColumn('name', 'full_name')->toSql(),
        );
    }

    public function test_rename_constraint_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE users RENAME CONSTRAINT pk_old TO pk_new',
            AlterTableBuilder::create('users')->renameConstraint('pk_old', 'pk_new')->toSql(),
        );
    }

    public function test_rename_table_builder_implements_sql_query(): void
    {
        static::assertInstanceOf(Sql::class, RenameTableBuilder::renameTo('users', null, 'people', false));
    }

    public function test_rename_table_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE users RENAME TO people',
            AlterTableBuilder::create('users')->renameTo('people')->toSql(),
        );
    }

    public function test_set_logged(): void
    {
        $builder = AlterTableBuilder::create('users')->setLogged();

        static::assertSame('ALTER TABLE users SET LOGGED', $builder->toSql());

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame('users', $ast->getRelation()->getRelname());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetLogged, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
    }

    public function test_set_logged_with_schema(): void
    {
        static::assertSame(
            'ALTER TABLE public.users SET LOGGED',
            AlterTableBuilder::create('users', 'public')->setLogged()->toSql(),
        );
    }

    public function test_set_tablespace(): void
    {
        $builder = AlterTableBuilder::create('users')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetTableSpace, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('fast_storage', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_set_tablespace_to_sql(): void
    {
        static::assertSame(
            'ALTER TABLE users SET TABLESPACE fast_storage',
            AlterTableBuilder::create('users')->setTablespace('fast_storage')->toSql(),
        );
    }

    public function test_set_unlogged(): void
    {
        $builder = AlterTableBuilder::create('users')->setUnlogged();

        static::assertSame('ALTER TABLE users SET UNLOGGED', $builder->toSql());

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame('users', $ast->getRelation()->getRelname());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetUnLogged, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
    }

    public function test_set_unlogged_if_exists(): void
    {
        static::assertSame(
            'ALTER TABLE IF EXISTS users SET UNLOGGED',
            AlterTableBuilder::create('users')->ifExists()->setUnlogged()->toSql(),
        );
    }

    public function test_simple_alter_table(): void
    {
        $builder = AlterTableBuilder::create('users')->addColumn(ColumnDefinition::create(
            'email',
            ColumnType::varchar(255),
        ));

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame('users', $ast->getRelation()->getRelname());
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertCount(1, $ast->getCmds());
    }
}
