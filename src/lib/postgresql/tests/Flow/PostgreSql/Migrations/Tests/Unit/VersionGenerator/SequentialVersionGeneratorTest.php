<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\VersionGenerator;

use Flow\PostgreSql\Migrations\{ExecutedMigration, Version};
use Flow\PostgreSql\Migrations\Store\{ExecutedMigrations, MigrationStore};
use Flow\PostgreSql\Migrations\VersionGenerator\SequentialVersionGenerator;
use PHPUnit\Framework\TestCase;

final class SequentialVersionGeneratorTest extends TestCase
{
    public function test_generates_first_version_when_store_is_empty() : void
    {
        $store = self::createStub(MigrationStore::class);
        $store->method('executedMigrations')->willReturn(new ExecutedMigrations());

        $generator = new SequentialVersionGenerator($store);

        self::assertSame('00001', (string) $generator->generate());
    }

    public function test_generates_version_with_custom_padding() : void
    {
        $store = self::createStub(MigrationStore::class);
        $store->method('executedMigrations')->willReturn(new ExecutedMigrations());

        $generator = new SequentialVersionGenerator($store, 3);

        self::assertSame('001', (string) $generator->generate());
    }

    public function test_increments_from_latest_version() : void
    {
        $store = self::createStub(MigrationStore::class);
        $store->method('executedMigrations')->willReturn(
            new ExecutedMigrations(
                new ExecutedMigration(Version::fromString('00001'), new \DateTimeImmutable(), 100),
                new ExecutedMigration(Version::fromString('00003'), new \DateTimeImmutable(), 200),
                new ExecutedMigration(Version::fromString('00002'), new \DateTimeImmutable(), 150),
            ),
        );

        $generator = new SequentialVersionGenerator($store);

        self::assertSame('00004', (string) $generator->generate());
    }
}
