<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Migrations\VersionResolver;
use PHPUnit\Framework\TestCase;

final class VersionResolverTest extends TestCase
{
    public function test_resolve_first(): void
    {
        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
        );

        static::assertTrue(Version::fromString('20260401120000')->equals($resolver->resolve('first')));
    }

    public function test_resolve_first_throws_when_empty(): void
    {
        $resolver = new VersionResolver(new FakeMigrationRepository(), new FakeMigrationStore());

        $this->expectException(MigrationException::class);
        $resolver->resolve('first');
    }

    public function test_resolve_latest(): void
    {
        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
        );

        static::assertTrue(Version::fromString('20260402120000')->equals($resolver->resolve('latest')));
    }

    public function test_resolve_literal_version(): void
    {
        $resolver = new VersionResolver(new FakeMigrationRepository(), new FakeMigrationStore());

        static::assertTrue(Version::fromString('20260401120000')->equals($resolver->resolve('20260401120000')));
    }

    public function test_resolve_next_with_executed(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);

        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
            ),
            $store,
        );

        static::assertTrue(Version::fromString('20260402120000')->equals($resolver->resolve('next')));
    }

    public function test_resolve_next_without_executed(): void
    {
        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
        );

        static::assertTrue(Version::fromString('20260401120000')->equals($resolver->resolve('next')));
    }

    public function test_resolve_prev(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $store->complete(Version::fromString('20260402120000'), 100);

        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
            ),
            $store,
        );

        static::assertTrue(Version::fromString('20260401120000')->equals($resolver->resolve('prev')));
    }

    public function test_resolve_prev_throws_when_no_executed(): void
    {
        $resolver = new VersionResolver(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
        );

        $this->expectException(MigrationException::class);
        $resolver->resolve('prev');
    }
}
