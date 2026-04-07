<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\{ExecutedMigration, Version};
use PHPUnit\Framework\TestCase;

final class ExecutedMigrationTest extends TestCase
{
    public function test_construction() : void
    {
        $version = Version::fromString('20260403120000');
        $executedAt = new \DateTimeImmutable('2026-04-03 12:00:00');

        $executed = new ExecutedMigration($version, $executedAt, 150);

        self::assertTrue($version->equals($executed->version));
        self::assertSame($executedAt, $executed->executedAt);
        self::assertSame(150, $executed->executionTimeMs);
    }

    public function test_construction_with_null_execution_time() : void
    {
        $executed = new ExecutedMigration(
            Version::fromString('20260403120000'),
            new \DateTimeImmutable('2026-04-03 12:00:00'),
            null,
        );

        self::assertNull($executed->executionTimeMs);
    }
}
