<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{col, eq, literal, schema_view, select, star, table};

use PHPUnit\Framework\TestCase;

final class ViewTest extends TestCase
{
    public function test_to_sql_generates_create_view() : void
    {
        self::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users',
            schema_view('active_users', select(star())->from(table('users'))->toSql())->toSql()->toSql(),
        );
    }

    public function test_view_construction() : void
    {
        $definition = select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql();
        $view = schema_view('active_users', $definition);

        self::assertSame('active_users', $view->name);
        self::assertSame($definition, $view->definition);
        self::assertFalse($view->isUpdatable);
    }

    public function test_view_updatable() : void
    {
        $view = schema_view('user_emails', select(col('id'), col('email'))->from(table('users'))->toSql(), isUpdatable: true);

        self::assertTrue($view->isUpdatable);
    }
}
