<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\{MigrationState, MigrationStatus, MigrationStatusList, Version};
use PHPUnit\Framework\TestCase;

final class MigrationStatusListTest extends TestCase
{
    public function test_count() : void
    {
        $list = new MigrationStatusList(
            new MigrationStatus(Version::fromString('20260401120000'), 'first', MigrationState::EXECUTED, new \DateTimeImmutable()),
            new MigrationStatus(Version::fromString('20260402120000'), 'second', MigrationState::PENDING, null),
            new MigrationStatus(Version::fromString('20260403120000'), 'third', MigrationState::UNAVAILABLE, new \DateTimeImmutable()),
        );

        self::assertCount(3, $list);
    }

    public function test_empty_list() : void
    {
        $list = new MigrationStatusList();

        self::assertCount(0, $list);
        self::assertTrue($list->isEmpty());
    }

    public function test_executed_filter() : void
    {
        $list = new MigrationStatusList(
            new MigrationStatus(Version::fromString('20260401120000'), 'first', MigrationState::EXECUTED, new \DateTimeImmutable()),
            new MigrationStatus(Version::fromString('20260402120000'), 'second', MigrationState::PENDING, null),
            new MigrationStatus(Version::fromString('20260403120000'), 'third', MigrationState::EXECUTED, new \DateTimeImmutable()),
        );

        $executed = $list->executed();

        self::assertCount(2, $executed);

        foreach ($executed as $status) {
            self::assertSame(MigrationState::EXECUTED, $status->state);
        }
    }

    public function test_iteration() : void
    {
        $s1 = new MigrationStatus(Version::fromString('20260401120000'), 'first', MigrationState::PENDING, null);
        $s2 = new MigrationStatus(Version::fromString('20260402120000'), 'second', MigrationState::EXECUTED, new \DateTimeImmutable());

        $list = new MigrationStatusList($s1, $s2);
        $items = \iterator_to_array($list);

        self::assertSame([$s1, $s2], $items);
    }

    public function test_pending_filter() : void
    {
        $list = new MigrationStatusList(
            new MigrationStatus(Version::fromString('20260401120000'), 'first', MigrationState::EXECUTED, new \DateTimeImmutable()),
            new MigrationStatus(Version::fromString('20260402120000'), 'second', MigrationState::PENDING, null),
            new MigrationStatus(Version::fromString('20260403120000'), 'third', MigrationState::PENDING, null),
        );

        $pending = $list->pending();

        self::assertCount(2, $pending);

        foreach ($pending as $status) {
            self::assertSame(MigrationState::PENDING, $status->state);
        }
    }
}
