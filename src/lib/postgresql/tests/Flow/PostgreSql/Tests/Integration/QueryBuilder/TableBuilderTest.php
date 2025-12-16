<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{
    alter,
    check_constraint,
    col,
    column,
    create,
    current_timestamp,
    data_type_boolean,
    data_type_integer,
    data_type_serial,
    data_type_text,
    data_type_timestamp,
    data_type_varchar,
    drop,
    foreign_key,
    primary_key,
    ref_action_cascade,
    ref_action_restrict,
    ref_action_set_null,
    select,
    table,
    truncate_table,
    unique_constraint
};

final class TableBuilderTest extends PGQueryTestCase
{
    public function test_alter_table_add_column() : void
    {
        $builder = alter()->table('users')
            ->addColumn(column('email', data_type_varchar(255))->notNull());

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users ADD COLUMN email varchar(255) NOT NULL'
        );
    }

    public function test_alter_table_add_constraint() : void
    {
        $builder = alter()->table('users')
            ->addConstraint(unique_constraint('email')->name('users_email_unique'));

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)'
        );
    }

    public function test_alter_table_alter_column_set_default() : void
    {
        $builder = alter()->table('users')
            ->alterColumnSetDefault('status', "'active'");

        $this->assertAlterTableQuery(
            $builder,
            "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'"
        );
    }

    public function test_alter_table_alter_column_set_not_null() : void
    {
        $builder = alter()->table('users')
            ->alterColumnSetNotNull('email');

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users ALTER COLUMN email SET NOT NULL'
        );
    }

    public function test_alter_table_alter_column_type() : void
    {
        $builder = alter()->table('users')
            ->alterColumnType('name', data_type_text());

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users ALTER COLUMN name TYPE pg_catalog.text'
        );
    }

    public function test_alter_table_drop_column() : void
    {
        $builder = alter()->table('users')
            ->dropColumn('temp_column');

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users DROP temp_column'
        );
    }

    public function test_alter_table_drop_column_cascade() : void
    {
        $builder = alter()->table('users')
            ->dropColumn('temp_column', cascade: true);

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users DROP temp_column CASCADE'
        );
    }

    public function test_alter_table_drop_constraint() : void
    {
        $builder = alter()->table('users')
            ->dropConstraint('users_email_unique');

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users DROP CONSTRAINT users_email_unique'
        );
    }

    public function test_alter_table_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->addColumn(column('email', data_type_varchar(255)));

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE IF EXISTS users ADD COLUMN email varchar(255)'
        );
    }

    public function test_alter_table_multiple_operations() : void
    {
        $builder = alter()->table('users')
            ->addColumn(column('phone', data_type_varchar(20)))
            ->dropColumn('fax')
            ->alterColumnSetNotNull('email');

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE users ADD COLUMN phone varchar(20), DROP fax, ALTER COLUMN email SET NOT NULL'
        );
    }

    public function test_alter_table_rename_column() : void
    {
        $builder = alter()->table('users')
            ->renameColumn('old_name', 'new_name');

        $this->assertRenameQuery(
            $builder,
            'ALTER TABLE users RENAME COLUMN old_name TO new_name'
        );
    }

    public function test_alter_table_rename_column_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->renameColumn('old_name', 'new_name');

        $this->assertRenameQuery(
            $builder,
            'ALTER TABLE IF EXISTS users RENAME COLUMN old_name TO new_name'
        );
    }

    public function test_alter_table_rename_column_with_schema() : void
    {
        $builder = alter()->table('users', 'public')
            ->renameColumn('old_name', 'new_name');

        $this->assertRenameQuery(
            $builder,
            'ALTER TABLE public.users RENAME COLUMN old_name TO new_name'
        );
    }

    public function test_alter_table_rename_constraint() : void
    {
        $builder = alter()->table('users')
            ->renameConstraint('old_constraint', 'new_constraint');

        $this->assertRenameQuery(
            $builder,
            'ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint'
        );
    }

    public function test_alter_table_rename_to() : void
    {
        $builder = alter()->table('users')
            ->renameTo('users_archive');

        $this->assertRenameQuery(
            $builder,
            'ALTER TABLE users RENAME TO users_archive'
        );
    }

    public function test_alter_table_set_schema() : void
    {
        $builder = alter()->table('users')
            ->setSchema('archive');

        $this->assertAlterObjectSchemaQuery(
            $builder,
            'ALTER TABLE users SET SCHEMA archive'
        );
    }

    public function test_alter_table_set_schema_if_exists() : void
    {
        $builder = alter()->table('users')
            ->ifExists()
            ->setSchema('archive');

        $this->assertAlterObjectSchemaQuery(
            $builder,
            'ALTER TABLE IF EXISTS users SET SCHEMA archive'
        );
    }

    public function test_alter_table_with_schema() : void
    {
        $builder = alter()->table('users', 'public')
            ->addColumn(column('email', data_type_varchar(255)));

        $this->assertAlterTableQuery(
            $builder,
            'ALTER TABLE public.users ADD COLUMN email varchar(255)'
        );
    }

    public function test_create_table_as_if_not_exists() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->ifNotExists();

        $this->assertCreateTableAsQuery(
            $builder,
            'CREATE TABLE IF NOT EXISTS users_backup AS SELECT id, name FROM users'
        );
    }

    public function test_create_table_as_with_column_names() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->columnNames('user_id', 'user_name');

        $this->assertCreateTableAsQuery(
            $builder,
            'CREATE TABLE users_backup(user_id, user_name) AS SELECT id, name FROM users'
        );
    }

    public function test_create_table_as_with_no_data() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder)
            ->withNoData();

        $this->assertCreateTableAsQuery(
            $builder,
            'CREATE TABLE users_backup AS SELECT id, name FROM users  WITH NO DATA'
        );
    }

    public function test_create_table_if_not_exists() : void
    {
        $builder = create()->table('users')
            ->ifNotExists()
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('name', data_type_varchar(100)));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY, name varchar(100))'
        );
    }

    public function test_create_table_simple() : void
    {
        $builder = create()->table('users')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('name', data_type_varchar(100))->notNull());

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE users (id serial PRIMARY KEY, name varchar(100) NOT NULL)'
        );
    }

    public function test_create_table_with_check_constraint() : void
    {
        $builder = create()->table('products')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('price', data_type_integer()))
            ->constraint(check_constraint('price > 0')->name('positive_price'));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE products (id serial PRIMARY KEY, price int, CONSTRAINT positive_price CHECK (price > 0))'
        );
    }

    public function test_create_table_with_column_default() : void
    {
        $builder = create()->table('users')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('active', data_type_boolean())->default(true));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE users (id serial PRIMARY KEY, active boolean DEFAULT true)'
        );
    }

    public function test_create_table_with_column_default_current_timestamp() : void
    {
        $builder = create()->table('audit_log')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('message', data_type_text())->notNull())
            ->column(column('created_at', data_type_timestamp())->notNull()->default(current_timestamp()));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE audit_log (id serial PRIMARY KEY, message pg_catalog.text NOT NULL, created_at timestamp NOT NULL DEFAULT current_timestamp)'
        );
    }

    public function test_create_table_with_composite_primary_key() : void
    {
        $builder = create()->table('order_items')
            ->column(column('order_id', data_type_integer())->notNull())
            ->column(column('product_id', data_type_integer())->notNull())
            ->column(column('quantity', data_type_integer()))
            ->constraint(primary_key('order_id', 'product_id'));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE order_items (order_id int NOT NULL, product_id int NOT NULL, quantity int, PRIMARY KEY (order_id, product_id))'
        );
    }

    public function test_create_table_with_foreign_key() : void
    {
        $builder = create()->table('orders')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('user_id', data_type_integer())->notNull())
            ->constraint(
                foreign_key(['user_id'], 'users', ['id'])
                    ->onDelete(ref_action_cascade())
                    ->onUpdate(ref_action_restrict())
            );

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE)'
        );
    }

    public function test_create_table_with_foreign_key_set_null() : void
    {
        $builder = create()->table('comments')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('user_id', data_type_integer()))
            ->constraint(
                foreign_key(['user_id'], 'users', ['id'])
                    ->onDelete(ref_action_set_null())
            );

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE comments (id serial PRIMARY KEY, user_id int, FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE SET NULL)'
        );
    }

    public function test_create_table_with_schema() : void
    {
        $builder = create()->table('users', 'public')
            ->column(column('id', data_type_serial())->primaryKey());

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE public.users (id serial PRIMARY KEY)'
        );
    }

    public function test_create_table_with_timestamp() : void
    {
        $builder = create()->table('audit_log')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('created_at', data_type_timestamp())->notNull())
            ->column(column('updated_at', data_type_timestamp()));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE audit_log (id serial PRIMARY KEY, created_at timestamp NOT NULL, updated_at timestamp)'
        );
    }

    public function test_create_table_with_unique_constraint() : void
    {
        $builder = create()->table('users')
            ->column(column('id', data_type_serial())->primaryKey())
            ->column(column('email', data_type_varchar(255))->notNull())
            ->constraint(unique_constraint('email'));

        $this->assertCreateTableQuery(
            $builder,
            'CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL, UNIQUE (email))'
        );
    }

    public function test_drop_table_cascade() : void
    {
        $builder = drop()->table('users')
            ->cascade();

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE users CASCADE'
        );
    }

    public function test_drop_table_if_exists() : void
    {
        $builder = drop()->table('users')
            ->ifExists();

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE IF EXISTS users'
        );
    }

    public function test_drop_table_if_exists_cascade() : void
    {
        $builder = drop()->table('users')
            ->ifExists()
            ->cascade();

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE IF EXISTS users CASCADE'
        );
    }

    public function test_drop_table_multiple_tables() : void
    {
        $builder = drop()->table('users', 'orders', 'products');

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE users, orders, products'
        );
    }

    public function test_drop_table_restrict() : void
    {
        $builder = drop()->table('users')
            ->restrict();

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE users'
        );
    }

    public function test_drop_table_simple() : void
    {
        $builder = drop()->table('users');

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE users'
        );
    }

    public function test_drop_table_with_schema() : void
    {
        $builder = drop()->table('public.users');

        $this->assertDropTableQuery(
            $builder,
            'DROP TABLE public.users'
        );
    }

    public function test_simple_create_table_as() : void
    {
        $selectBuilder = select()
            ->select(col('id'), col('name'))
            ->from(table('users'));

        $builder = create()->tableAs('users_backup', $selectBuilder);

        $this->assertCreateTableAsQuery(
            $builder,
            'CREATE TABLE users_backup AS SELECT id, name FROM users'
        );
    }

    public function test_truncate_cascade() : void
    {
        $builder = truncate_table('users')
            ->cascade();

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users CASCADE'
        );
    }

    public function test_truncate_multiple_tables() : void
    {
        $builder = truncate_table('users', 'orders', 'products');

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users, orders, products'
        );
    }

    public function test_truncate_restart_identity() : void
    {
        $builder = truncate_table('users')
            ->restartIdentity();

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users RESTART IDENTITY'
        );
    }

    public function test_truncate_restart_identity_cascade() : void
    {
        $builder = truncate_table('users')
            ->restartIdentity()
            ->cascade();

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users RESTART IDENTITY CASCADE'
        );
    }

    public function test_truncate_restrict() : void
    {
        $builder = truncate_table('users')
            ->restrict();

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users'
        );
    }

    public function test_truncate_simple() : void
    {
        $builder = truncate_table('users');

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE users'
        );
    }

    public function test_truncate_with_schema() : void
    {
        $builder = truncate_table('public.users');

        $this->assertTruncateQuery(
            $builder,
            'TRUNCATE public.users'
        );
    }
}
