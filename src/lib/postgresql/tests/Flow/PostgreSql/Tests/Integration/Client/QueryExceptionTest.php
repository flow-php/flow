<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{alter, cast, check_constraint, col, column_type_integer, create, foreign_key, gt, insert, literal, select};
use Flow\PostgreSql\Client\Exception\{PostgreSqlErrorCategory, QueryException};
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, ColumnType};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class QueryExceptionTest extends PostgreSqlTestCase
{
    public function test_check_constraint_violation() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_check_violation')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
                ->column(ColumnDefinition::create('age', ColumnType::integer()))
        );
        $this->pgsqlContext()->client()->execute(
            alter()->table('test_check_violation')
                ->addConstraint(check_constraint(gt(col('age'), literal(0)))->name('age_positive'))
        );

        try {
            $this->pgsqlContext()->client()->execute(
                insert()->into('test_check_violation')
                    ->columns('id', 'age')
                    ->values(literal(1), literal(-5))
            );
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('23514', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
            self::assertNotNull($error->schema);
            self::assertMatchesRegularExpression('/^pg_temp(_\d+)?$/', $error->schema);
            self::assertSame('test_check_violation', $error->table);
            self::assertSame('age_positive', $error->constraint);
        }
    }

    public function test_data_exception() : void
    {
        try {
            $this->pgsqlContext()->client()->execute(select(cast(literal('not_a_number'), column_type_integer())));
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('22P02', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::DATA_EXCEPTION, $error->category);
            self::assertSame('invalid input syntax for type integer: "not_a_number"', $error->message);
        }
    }

    public function test_exception_message_format() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_safe_message')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
        );
        $this->pgsqlContext()->client()->execute(
            insert()->into('test_safe_message')->columns('id')->values(literal(1))
        );

        try {
            $this->pgsqlContext()->client()->execute(
                insert()->into('test_safe_message')->columns('id')->values(literal(1))
            );
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            self::assertStringContainsString('[23505]', $e->getMessage());
            self::assertStringContainsString('Data constraint violation', $e->getMessage());
        }
    }

    public function test_foreign_key_violation() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_fk_parent')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
        );
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_fk_child')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
                ->column(ColumnDefinition::create('parent_id', ColumnType::integer())->notNull())
        );
        $this->pgsqlContext()->client()->execute(
            alter()->table('test_fk_child')
                ->addConstraint(foreign_key(['parent_id'], 'test_fk_parent', ['id'])->name('test_fk'))
        );

        try {
            $this->pgsqlContext()->client()->execute(
                insert()->into('test_fk_child')
                    ->columns('id', 'parent_id')
                    ->values(literal(1), literal(999))
            );
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('23503', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
            self::assertNotNull($error->schema);
            self::assertMatchesRegularExpression('/^pg_temp(_\d+)?$/', $error->schema);
            self::assertSame('test_fk_child', $error->table);
            self::assertSame('test_fk', $error->constraint);
        }
    }

    public function test_not_null_violation() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_not_null_violation')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
                ->column(ColumnDefinition::create('name', ColumnType::text())->notNull())
        );

        try {
            $this->pgsqlContext()->client()->execute(
                insert()->into('test_not_null_violation')
                    ->columns('id', 'name')
                    ->values(literal(1), literal(null))
            );
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('23502', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
            self::assertNotNull($error->schema);
            self::assertMatchesRegularExpression('/^pg_temp(_\d+)?$/', $error->schema);
            self::assertSame('test_not_null_violation', $error->table);
            self::assertSame('name', $error->column);
        }
    }

    public function test_sql_is_accessible() : void
    {
        try {
            $this->pgsqlContext()->client()->execute('SELECT * FORM invalid_syntax');
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            self::assertSame('SELECT * FORM invalid_syntax', $e->sql());
        }
    }

    public function test_syntax_error() : void
    {
        try {
            $this->pgsqlContext()->client()->execute('SELECT * FORM users');
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('42601', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, $error->category);
            self::assertSame(10, $error->position);
            self::assertSame('syntax error at or near "FORM"', $error->message);
        }
    }

    public function test_unique_violation() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_unique_violation')
                ->column(ColumnDefinition::create('id', ColumnType::integer())->primaryKey())
                ->column(ColumnDefinition::create('email', ColumnType::text())->unique())
        );
        $this->pgsqlContext()->client()->execute(
            insert()->into('test_unique_violation')
                ->columns('id', 'email')
                ->values(literal(1), literal('test@example.com'))
        );

        try {
            $this->pgsqlContext()->client()->execute(
                insert()->into('test_unique_violation')
                    ->columns('id', 'email')
                    ->values(literal(2), literal('test@example.com'))
            );
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            $error = $e->error();

            self::assertSame('23505', $error->sqlState);
            self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
            self::assertNotNull($error->schema);
            self::assertMatchesRegularExpression('/^pg_temp(_\d+)?$/', $error->schema);
            self::assertSame('test_unique_violation', $error->table);
            self::assertSame('test_unique_violation_email_key', $error->constraint);
            self::assertSame('duplicate key value violates unique constraint "test_unique_violation_email_key"', $error->message);
        }
    }
}
