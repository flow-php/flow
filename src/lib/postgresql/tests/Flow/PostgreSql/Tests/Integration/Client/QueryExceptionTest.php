<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{create, insert, literal};
use Flow\PostgreSql\Client\Exception\{PostgreSqlErrorCategory, QueryException};
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, DataType};

final class QueryExceptionTest extends ClientTestCase
{
    public function test_check_constraint_violation() : void
    {
        $this->client->execute(
            create()->temporaryTable('test_check_violation')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
                ->column(ColumnDefinition::create('age', DataType::integer()))
        );
        $this->client->execute('ALTER TABLE test_check_violation ADD CONSTRAINT age_positive CHECK (age > 0)');

        try {
            $this->client->execute(
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
            $this->client->execute("SELECT 'not_a_number'::integer");
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
        $this->client->execute(
            create()->temporaryTable('test_safe_message')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
        );
        $this->client->execute(
            insert()->into('test_safe_message')->columns('id')->values(literal(1))
        );

        try {
            $this->client->execute(
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
        $this->client->execute(
            create()->temporaryTable('test_fk_parent')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
        );
        $this->client->execute(
            create()->temporaryTable('test_fk_child')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
                ->column(ColumnDefinition::create('parent_id', DataType::integer())->notNull())
        );
        $this->client->execute('ALTER TABLE test_fk_child ADD CONSTRAINT test_fk FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)');

        try {
            $this->client->execute(
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
        $this->client->execute(
            create()->temporaryTable('test_not_null_violation')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
                ->column(ColumnDefinition::create('name', DataType::text())->notNull())
        );

        try {
            $this->client->execute(
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
            $this->client->execute('SELECT * FORM invalid_syntax');
            self::fail('Expected QueryException to be thrown');
        } catch (QueryException $e) {
            self::assertSame('SELECT * FORM invalid_syntax', $e->sql());
        }
    }

    public function test_syntax_error() : void
    {
        try {
            $this->client->execute('SELECT * FORM users');
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
        $this->client->execute(
            create()->temporaryTable('test_unique_violation')
                ->column(ColumnDefinition::create('id', DataType::integer())->primaryKey())
                ->column(ColumnDefinition::create('email', DataType::text())->unique())
        );
        $this->client->execute(
            insert()->into('test_unique_violation')
                ->columns('id', 'email')
                ->values(literal(1), literal('test@example.com'))
        );

        try {
            $this->client->execute(
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
