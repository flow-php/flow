<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\ExecutedMigration;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class ExecutedMigrationTest extends TestCase
{
    public function test_construction(): void
    {
        $version = Version::fromString('20260403120000');
        $executedAt = new \DateTimeImmutable('2026-04-03 12:00:00');

        $executed = new ExecutedMigration($version, $executedAt, 150);

        static::assertTrue($version->equals($executed->version));
        static::assertSame($executedAt, $executed->executedAt);
        static::assertSame(150, $executed->executionTimeMs);
    }

    public function test_construction_with_null_execution_time(): void
    {
        $executed = new ExecutedMigration(
            Version::fromString('20260403120000'),
            new \DateTimeImmutable('2026-04-03 12:00:00'),
            null,
        );

        static::assertNull($executed->executionTimeMs);
    }
}
