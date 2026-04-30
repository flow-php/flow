<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\{DataAccessException, NoResultException};
use PHPUnit\Framework\TestCase;

final class NoResultExceptionTest extends TestCase
{
    public function test_default_message() : void
    {
        self::assertSame(
            'Expected at least one row, but none were returned',
            (new NoResultException())->getMessage(),
        );
    }

    public function test_extends_data_access_exception() : void
    {
        self::assertInstanceOf(DataAccessException::class, new NoResultException());
    }
}
