<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\DataAccessException;
use Flow\PostgreSql\Client\Exception\NoResultException;
use PHPUnit\Framework\TestCase;

final class NoResultExceptionTest extends TestCase
{
    public function test_default_message(): void
    {
        static::assertSame(
            'Expected at least one row, but none were returned',
            (new NoResultException())->getMessage(),
        );
    }

    public function test_extends_data_access_exception(): void
    {
        static::assertInstanceOf(DataAccessException::class, new NoResultException());
    }
}
