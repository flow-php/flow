<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Lock;

use Flow\PostgreSql\Migrations\Lock\NoMigrationLock;
use PHPUnit\Framework\TestCase;

final class NoMigrationLockTest extends TestCase
{
    public function test_always_acquires(): void
    {
        $lock = new NoMigrationLock();

        static::assertTrue($lock->acquire('public.flow_migrations'));
        static::assertTrue($lock->acquire('public.flow_migrations'));
    }

    public function test_release_without_acquire_does_not_throw(): void
    {
        $this->expectNotToPerformAssertions();

        (new NoMigrationLock())->release('public.flow_migrations');
    }
}
