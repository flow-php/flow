<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit\Dialect;

use Doctrine\DBAL\Platforms\SQLitePlatform;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use PHPUnit\Framework\TestCase;

final class SqliteDialectTest extends TestCase
{
    public function test_max_bind_parameters(): void
    {
        static::assertSame(32_766, (new SqliteDialect(new SQLitePlatform()))->maxBindParameters());
    }
}
