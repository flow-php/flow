<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use function Flow\PostgreSql\DSL\{count_all, select, table};

use PHPUnit\Framework\TestCase;

final class CountAllTest extends TestCase
{
    public function test_count_all_in_select() : void
    {
        self::assertSame(
            'SELECT count(*) FROM users',
            select(count_all())->from(table('users'))->toSql(),
        );
    }

    public function test_count_all_with_alias() : void
    {
        self::assertSame(
            'SELECT count(*) AS total FROM users',
            select(count_all()->as('total'))->from(table('users'))->toSql(),
        );
    }
}
