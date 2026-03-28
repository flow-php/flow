<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use function Flow\PostgreSql\DSL\{
    alter,
    check_constraint,
    col,
    column,
    column_type_boolean,
    column_type_integer,
    column_type_serial,
    column_type_text,
    column_type_timestamp,
    column_type_varchar,
    create,
    current_timestamp,
    drop,
    foreign_key,
    gt,
    literal,
    primary_key,
    ref_action_cascade,
    ref_action_restrict,
    ref_action_set_null,
    select,
    table,
    truncate_table,
    unique_constraint
};

use PHPUnit\Framework\TestCase;

final class TableBuilderTest extends TestCase
{
    public function test_alter_table_add_column() : void
    {
        $builder = alter()->table('users')
            ->addColumn(column('email', column_type_varchar(255))->notNull());

        self::assertSame(
            'ALTER TABLE users ADD COLUMN email varchar(255) NOT NULL',
            $builder->toSql()
        );
    }

    public function test_alter_table_add_constraint() : void
    {
        $builder = alter()->table('users')
            ->addConstraint(unique_constraint('email')->name('users_email_unique'));

        self::assertSame(
            'ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)',
            $builder->toSql()
        );
    }

    public function test_alter_table_alter_column_set_default() : void
    {
        $builder = alter()->table('users')
            ->alterColumnSetDefault('status', literal('active'));

        self::assertSame(
            "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'",
            $builder->toSql()
        );
    }

    public function test_alter_table_alter_column_set_not_null() : void
    {
        $builder = alter()->table('users')
            ->alterColumnSetNotNull('email');

        self::assertSame(
            'ALTER TABLE users ALTER COLUMN email SET NOT NULL',
            $builder->toSql()
        );
    }

    public function test_alter_table_alter_column_type() : void
    {
        $builder = alter()->table('users')
            ->alterColumnType('name', column_type_text());

        self::assertSame(
            'ALTER TABLE users ALTER COLUMN name TYPE pg_catalog.text',
            $builder->toSql()
        );
    }

    public function test_alter_table_drop_column() : void
    {
        $builder = alter()->table('users')
            ->dropColumn('temp_column');

        self::assertSame(
            'ALTER TABLE users DROP temp_column',
            $builder->toSql()
        );
    }

    public function test_alter_table_drop_column_cascade() : void
    {
        $builder = alter()->table('users')
            ->dropColumn('temp_column', cascade: true);

        self::assertSame(
            'ALTER TABLE users DROP temp_column CASCADE',
            $builder->toSql()
        );
    }

    public function test_alter_table_drop_constraint() : void
    {
        $builder = alter()->table('users')
            ->dropConstraint('users_email_unique');

        self::assertSame(
            'ALTER TABLE users DROP CONSTRAINT users_email_unique',
            $builder->toSql()
        );
    }

    public function test_alter_table_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->addColumn(column('email', column_type_varchar(255)));

        self::assertSame(
            'ALTER TABLE IF EXISTS users ADD COLUMN email varchar(255)',
            $builder->toSql()
        );
    }

    public function test_alter_table_multiple_operations() : void
    {
        $builder = alter()->table('users')
            ->addColumn(column('phone', column_type_varchar(20)))
            ->dropColumn('fax')
            ->alterColumnSetNotNull('email');

        self::assertSame(
            'ALTER TABLE users ADD COLUMN phone varchar(20), DROP fax, ALTER COLUMN email SET NOT NULL',
            $builder->toSql()
        );
    }

    public function test_alter_table_rename_column() : void
    {
        $builder = alter()->table('users')
            ->renameColumn('old_name', 'new_name');

        self::assertSame(
            'ALTER TABLE users RENAME COLUMN old_name TO new_name',
            $builder->toSql()
        );
    }

    public function test_alter_table_rename_column_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->renameColumn('old_name', 'new_name');

        self::assertSame(
            'ALTER TABLE IF EXISTS users RENAME COLUMN old_name TO new_name',
            $builder->toSql()
        );
    }

    public function test_alter_table_rename_column_with_schema() : void
    {
        $builder = alter()->table('users', 'public')
            ->renameColumn('old_name', 'new_name');

        self::assertSame(
            'ALTER TABLE public.users RENAME COLUMN old_name TO new_name',
            $builder->toSql()
        );
    }

    public function test_alter_table_rename_constraint() : void
    {
        $builder = alter()->table('users')
            ->renameConstraint('old_constraint', 'new_constraint');

        self::assertSame(
            'ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint',
            $builder->toSql()
        );
    }

    public function test_alter_table_rename_to() : void
    {
        $builder = alter()->table('users')
            ->renameTo('users_archive');

        self::assertSame(
            'ALTER TABLE users RENAME TO users_archive',
            $builder->toSql()
        );
    }

    public function test_alter_table_set_schema() : void
    {
        $builder = alter()->table('users')
            ->setSchema('archive');

        self::assertSame(
            'ALTER TABLE users SET SCHEMA archive',
            $builder->toSql()
        );
    }

    public function test_alter_table_set_schema_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->setSchema('archive');

        self::assertSame(
            'ALTER TABLE IF EXISTS users SET SCHEMA archive',
            $builder->toSql()
        );
    }

    public function test_alter_table_with_schema() : void
    {
        $builder = alter()->table('users', 'public')
            ->addColumn(column('email', column_type_varchar(255)));

        self::assertSame(
            'ALTER TABLE public.users ADD COLUMN email varchar(255)',
            $builder->toSql()
        );
    }

    public function test_create_table_as_if_not_exists() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->ifNotExists();

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS users_backup AS SELECT id, name FROM users',
            $builder->toSql()
        );
    }

    public function test_create_table_as_with_column_names() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->columnNames('user_id', 'user_name');

        self::assertSame(
            'CREATE TABLE users_backup(user_id, user_name) AS SELECT id, name FROM users',
            $builder->toSql()
        );
    }

    public function test_create_table_as_with_no_data() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->withNoData();

        self::assertSame(
            'CREATE TABLE users_backup AS SELECT id, name FROM users  WITH NO DATA',
            $builder->toSql()
        );
    }

    public function test_create_table_if_not_exists() : void
    {
        $builder = create()->table('users')
            ->ifNotExists()
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('name', column_type_varchar(100)));

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY, name varchar(100))',
            $builder->toSql()
        );
    }

    public function test_create_table_simple() : void
    {
        $builder = create()->table('users')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('name', column_type_varchar(100))->notNull());

        self::assertSame(
            'CREATE TABLE users (id serial PRIMARY KEY, name varchar(100) NOT NULL)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_check_constraint() : void
    {
        $builder = create()->table('products')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('price', column_type_integer()))
            ->constraint(check_constraint(gt(col('price'), literal(0)))->name('positive_price'));

        self::assertSame(
            'CREATE TABLE products (id serial PRIMARY KEY, price int, CONSTRAINT positive_price CHECK (price > 0))',
            $builder->toSql()
        );
    }

    public function test_create_table_with_column_default() : void
    {
        $builder = create()->table('users')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('active', column_type_boolean())->default(true));

        self::assertSame(
            'CREATE TABLE users (id serial PRIMARY KEY, active boolean DEFAULT true)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_column_default_current_timestamp() : void
    {
        $builder = create()->table('audit_log')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('message', column_type_text())->notNull())
            ->column(column('created_at', column_type_timestamp())->notNull()->default(current_timestamp()));

        self::assertSame(
            'CREATE TABLE audit_log (id serial PRIMARY KEY, message pg_catalog.text NOT NULL, created_at timestamp NOT NULL DEFAULT current_timestamp)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_composite_primary_key() : void
    {
        $builder = create()->table('order_items')
            ->column(column('order_id', column_type_integer())->notNull())
            ->column(column('product_id', column_type_integer())->notNull())
            ->column(column('quantity', column_type_integer()))
            ->constraint(primary_key('order_id', 'product_id'));

        self::assertSame(
            'CREATE TABLE order_items (order_id int NOT NULL, product_id int NOT NULL, quantity int, PRIMARY KEY (order_id, product_id))',
            $builder->toSql()
        );
    }

    public function test_create_table_with_foreign_key() : void
    {
        $builder = create()->table('orders')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('user_id', column_type_integer())->notNull())
            ->constraint(
                foreign_key(['user_id'], 'users', ['id'])
                    ->onDelete(ref_action_cascade())
                    ->onUpdate(ref_action_restrict())
            );

        self::assertSame(
            'CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_foreign_key_set_null() : void
    {
        $builder = create()->table('comments')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('user_id', column_type_integer()))
            ->constraint(
                foreign_key(['user_id'], 'users', ['id'])
                    ->onDelete(ref_action_set_null())
            );

        self::assertSame(
            'CREATE TABLE comments (id serial PRIMARY KEY, user_id int, FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE SET NULL)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_schema() : void
    {
        $builder = create()->table('users', 'public')
            ->column(column('id', column_type_serial())->primaryKey());

        self::assertSame(
            'CREATE TABLE public.users (id serial PRIMARY KEY)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_timestamp() : void
    {
        $builder = create()->table('audit_log')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('created_at', column_type_timestamp())->notNull())
            ->column(column('updated_at', column_type_timestamp()));

        self::assertSame(
            'CREATE TABLE audit_log (id serial PRIMARY KEY, created_at timestamp NOT NULL, updated_at timestamp)',
            $builder->toSql()
        );
    }

    public function test_create_table_with_unique_constraint() : void
    {
        $builder = create()->table('users')
            ->column(column('id', column_type_serial())->primaryKey())
            ->column(column('email', column_type_varchar(255))->notNull())
            ->constraint(unique_constraint('email'));

        self::assertSame(
            'CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL, UNIQUE (email))',
            $builder->toSql()
        );
    }

    public function test_drop_table_cascade() : void
    {
        $builder = drop()->table('users')
            ->cascade();

        self::assertSame(
            'DROP TABLE users CASCADE',
            $builder->toSql()
        );
    }

    public function test_drop_table_if_exists() : void
    {
        $builder = drop()->table('users')
            ->ifExists();

        self::assertSame(
            'DROP TABLE IF EXISTS users',
            $builder->toSql()
        );
    }

    public function test_drop_table_if_exists_cascade() : void
    {
        $builder = drop()->table('users')
            ->ifExists()
            ->cascade();

        self::assertSame(
            'DROP TABLE IF EXISTS users CASCADE',
            $builder->toSql()
        );
    }

    public function test_drop_table_multiple_tables() : void
    {
        $builder = drop()->table('users', 'orders', 'products');

        self::assertSame(
            'DROP TABLE users, orders, products',
            $builder->toSql()
        );
    }

    public function test_drop_table_restrict() : void
    {
        $builder = drop()->table('users')
            ->restrict();

        self::assertSame(
            'DROP TABLE users',
            $builder->toSql()
        );
    }

    public function test_drop_table_simple() : void
    {
        $builder = drop()->table('users');

        self::assertSame(
            'DROP TABLE users',
            $builder->toSql()
        );
    }

    public function test_drop_table_with_schema() : void
    {
        $builder = drop()->table('public.users');

        self::assertSame(
            'DROP TABLE public.users',
            $builder->toSql()
        );
    }

    public function test_simple_create_table_as() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder);

        self::assertSame(
            'CREATE TABLE users_backup AS SELECT id, name FROM users',
            $builder->toSql()
        );
    }

    public function test_truncate_cascade() : void
    {
        $builder = truncate_table('users')
            ->cascade();

        self::assertSame(
            'TRUNCATE users CASCADE',
            $builder->toSql()
        );
    }

    public function test_truncate_multiple_tables() : void
    {
        $builder = truncate_table('users', 'orders', 'products');

        self::assertSame(
            'TRUNCATE users, orders, products',
            $builder->toSql()
        );
    }

    public function test_truncate_restart_identity() : void
    {
        $builder = truncate_table('users')
            ->restartIdentity();

        self::assertSame(
            'TRUNCATE users RESTART IDENTITY',
            $builder->toSql()
        );
    }

    public function test_truncate_restart_identity_cascade() : void
    {
        $builder = truncate_table('users')
            ->restartIdentity()
            ->cascade();

        self::assertSame(
            'TRUNCATE users RESTART IDENTITY CASCADE',
            $builder->toSql()
        );
    }

    public function test_truncate_restrict() : void
    {
        $builder = truncate_table('users')
            ->restrict();

        self::assertSame(
            'TRUNCATE users',
            $builder->toSql()
        );
    }

    public function test_truncate_simple() : void
    {
        $builder = truncate_table('users');

        self::assertSame(
            'TRUNCATE users',
            $builder->toSql()
        );
    }

    public function test_truncate_with_schema() : void
    {
        $builder = truncate_table('public.users');

        self::assertSame(
            'TRUNCATE public.users',
            $builder->toSql()
        );
    }
}
