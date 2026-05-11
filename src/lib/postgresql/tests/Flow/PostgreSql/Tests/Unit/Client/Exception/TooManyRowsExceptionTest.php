<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\DataAccessException;
use Flow\PostgreSql\Client\Exception\TooManyRowsException;
use PHPUnit\Framework\TestCase;

final class TooManyRowsExceptionTest extends TestCase
{
    public function test_count_in_message(): void
    {
        static::assertSame(
            'Expected at most one row, but 5 were returned',
            (new TooManyRowsException(5))->getMessage(),
        );
    }

    public function test_extends_data_access_exception(): void
    {
        static::assertInstanceOf(DataAccessException::class, new TooManyRowsException(2));
    }
}
