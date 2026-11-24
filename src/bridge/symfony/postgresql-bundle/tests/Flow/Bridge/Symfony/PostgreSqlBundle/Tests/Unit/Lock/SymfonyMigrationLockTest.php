<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Lock;

use Flow\Bridge\Symfony\PostgreSqlBundle\Lock\SymfonyMigrationLock;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double\ForgetfulLockStore;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\Store\InMemoryStore;

final class SymfonyMigrationLockTest extends TestCase
{
    public function test_acquire_and_release(): void
    {
        $lock = new SymfonyMigrationLock(new LockFactory(new InMemoryStore()));

        static::assertTrue($lock->acquire('public.flow_migrations'));

        $lock->release('public.flow_migrations');

        static::assertTrue($lock->acquire('public.flow_migrations'));
    }

    public function test_release_of_an_expired_lock_throws(): void
    {
        $store = new ForgetfulLockStore();
        $lock = new SymfonyMigrationLock(new LockFactory($store));

        static::assertTrue($lock->acquire('public.flow_migrations'));

        $store->exists = false;

        $this->expectExceptionObject(MigrationException::lockExpired('public.flow_migrations'));

        $lock->release('public.flow_migrations');
    }

    public function test_release_without_acquire_throws(): void
    {
        $this->expectExceptionObject(MigrationException::lockExpired('public.flow_migrations'));

        (new SymfonyMigrationLock(new LockFactory(new InMemoryStore())))->release('public.flow_migrations');
    }

    public function test_second_lock_on_the_same_store_acquires_after_release(): void
    {
        $factory = new LockFactory(new InMemoryStore());
        $first = new SymfonyMigrationLock($factory);
        $second = new SymfonyMigrationLock($factory);

        static::assertTrue($first->acquire('public.flow_migrations'));
        static::assertFalse($second->acquire('public.flow_migrations'));

        $first->release('public.flow_migrations');

        static::assertTrue($second->acquire('public.flow_migrations'));
    }
}
