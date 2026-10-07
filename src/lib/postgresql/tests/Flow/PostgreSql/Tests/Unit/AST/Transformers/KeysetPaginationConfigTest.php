<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\KeysetPaginationConfig;
use Flow\PostgreSql\Exception\PaginationException;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\sql_keyset_column;

final class KeysetPaginationConfigTest extends TestCase
{
    public function test_null_cursor_value_is_rejected(): void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset cursor value #2 is NULL; key columns must be non-null');

        new KeysetPaginationConfig(
            10,
            [sql_keyset_column('created_at'), sql_keyset_column('id')],
            ['2025-01-01', null],
        );
    }

    public function test_parameter_cursor_is_accepted(): void
    {
        static::assertEquals(param(2), (new KeysetPaginationConfig(10, [sql_keyset_column('id')], param(2)))->cursor);
    }

    public function test_scalar_cursor_values_are_accepted(): void
    {
        static::assertSame(
            ['a', 1, 1.5, true],
            (new KeysetPaginationConfig(10, [sql_keyset_column('id')], ['a', 1, 1.5, true]))->cursor,
        );
    }
}
