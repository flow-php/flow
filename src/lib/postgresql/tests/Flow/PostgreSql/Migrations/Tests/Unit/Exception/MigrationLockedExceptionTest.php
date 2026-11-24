<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Exception;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Exception\MigrationLockedException;
use PHPUnit\Framework\TestCase;

final class MigrationLockedExceptionTest extends TestCase
{
    public function test_message_names_the_lock(): void
    {
        static::assertSame(
            'Another process is running migrations (lock "public.flow_migrations"). Run the command again when it finishes.',
            (new MigrationLockedException('public.flow_migrations'))->getMessage(),
        );
    }

    public function test_is_a_migration_exception(): void
    {
        static::assertInstanceOf(MigrationException::class, new MigrationLockedException('public.flow_migrations'));
    }
}
