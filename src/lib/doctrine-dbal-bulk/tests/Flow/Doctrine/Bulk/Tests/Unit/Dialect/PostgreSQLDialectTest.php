<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit\Dialect;

use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use PHPUnit\Framework\TestCase;

final class PostgreSQLDialectTest extends TestCase
{
    public function test_max_bind_parameters(): void
    {
        static::assertSame(65_535, (new PostgreSQLDialect(new PostgreSQLPlatform()))->maxBindParameters());
    }
}
