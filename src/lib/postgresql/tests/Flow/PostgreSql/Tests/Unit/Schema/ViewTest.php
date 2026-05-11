<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class ViewTest extends TestCase
{
    public function test_to_sql_generates_create_view(): void
    {
        static::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users',
            schema_view('active_users', select(star())->from(table('users'))->toSql())->toSql()->toSql(),
        );
    }

    public function test_view_construction(): void
    {
        $definition = select(star())
            ->from(table('users'))
            ->where(eq(col('active'), literal(true)))
            ->toSql();
        $view = schema_view('active_users', $definition);

        static::assertSame('active_users', $view->name);
        static::assertSame($definition, $view->definition);
        static::assertFalse($view->isUpdatable);
    }

    public function test_view_updatable(): void
    {
        $view = schema_view(
            'user_emails',
            select(col('id'), col('email'))->from(table('users'))->toSql(),
            isUpdatable: true,
        );

        static::assertTrue($view->isUpdatable);
    }
}
