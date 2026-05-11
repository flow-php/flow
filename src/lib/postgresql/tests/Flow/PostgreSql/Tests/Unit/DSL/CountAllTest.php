<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\count_all;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class CountAllTest extends TestCase
{
    public function test_count_all_in_select(): void
    {
        static::assertSame('SELECT count(*) FROM users', select(count_all())->from(table('users'))->toSql());
    }

    public function test_count_all_with_alias(): void
    {
        static::assertSame(
            'SELECT count(*) AS total FROM users',
            select(count_all()->as('total'))->from(table('users'))->toSql(),
        );
    }
}
