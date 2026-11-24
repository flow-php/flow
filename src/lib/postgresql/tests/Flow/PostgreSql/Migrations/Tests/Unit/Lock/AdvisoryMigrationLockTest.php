<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Lock;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Lock\AdvisoryMigrationLock;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class AdvisoryMigrationLockTest extends TestCase
{
    public function test_key_is_the_xxh64_hash_of_the_name(): void
    {
        $client = new SpyClient();

        (new AdvisoryMigrationLock($client))->acquire('public.flow_migrations');

        static::assertSame([8752161640987805066], $client->scalarQueries[0]['parameters']);
    }

    public function test_release_queries_pg_advisory_unlock(): void
    {
        $client = new SpyClient();

        (new AdvisoryMigrationLock($client))->release('public.flow_migrations');

        static::assertSame(
            [['sql' => 'SELECT pg_advisory_unlock($1)', 'parameters' => [8752161640987805066]]],
            $client->scalarQueries,
        );
    }

    public function test_release_throws_when_the_lock_was_not_held(): void
    {
        $client = new SpyClient();
        $client->scalarBoolResults = [false];

        $this->expectExceptionObject(MigrationException::advisoryLockNotHeld('public.flow_migrations'));

        (new AdvisoryMigrationLock($client))->release('public.flow_migrations');
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_acquire_returns_the_query_result(bool $acquired): void
    {
        $client = new SpyClient();
        $client->scalarBoolResults = [$acquired];

        static::assertSame($acquired, (new AdvisoryMigrationLock($client))->acquire('public.flow_migrations'));
        static::assertSame('SELECT pg_try_advisory_lock($1)', $client->scalarQueries[0]['sql']);
    }
}
