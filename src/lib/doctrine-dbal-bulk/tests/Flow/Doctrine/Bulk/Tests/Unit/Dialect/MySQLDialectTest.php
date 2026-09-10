<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit\Dialect;

use Doctrine\DBAL\Platforms\MySQLPlatform;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use PHPUnit\Framework\TestCase;

final class MySQLDialectTest extends TestCase
{
    public function test_max_bind_parameters(): void
    {
        static::assertSame(65_535, (new MySQLDialect(new MySQLPlatform()))->maxBindParameters());
    }
}
