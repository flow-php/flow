<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration\Lock;

use Flow\PostgreSql\Migrations\Lock\AdvisoryMigrationLock;
use Flow\PostgreSql\Migrations\Tests\Integration\PostgreSqlMigrationsContext;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;

final class AdvisoryMigrationLockTest extends TestCase
{
    protected PostgreSqlMigrationsContext $context;

    protected function setUp(): void
    {
        $this->context = new PostgreSqlMigrationsContext();
        $this->context->dropTableIfExists('public.flow_migrations_lock_test');
    }

    protected function tearDown(): void
    {
        $this->context->dropTableIfExists('public.flow_migrations_lock_test');
        $this->context->close();
    }

    public function test_concurrent_index_build_is_not_blocked_by_a_session_that_tries_the_lock(): void
    {
        $this->context->client->execute(
            create()->table('flow_migrations_lock_test')->column(column('id', column_type_integer())),
        );
        $this->context->client->execute(
            insert()
                ->into('flow_migrations_lock_test')
                ->columns('id')
                ->select(select(func('generate_series', [literal(1), literal(1000)]))),
        );

        $first = new AdvisoryMigrationLock($this->context->client);
        $second = new AdvisoryMigrationLock($this->context->secondClient());

        static::assertTrue($first->acquire('public.flow_migrations_lock_test'));
        static::assertFalse($second->acquire('public.flow_migrations_lock_test'));

        $this->context->client->execute(
            create()
                ->index('flow_migrations_lock_test_id_idx')
                ->concurrently()
                ->on('flow_migrations_lock_test')
                ->columns('id'),
        );

        $first->release('public.flow_migrations_lock_test');

        static::assertTrue($second->acquire('public.flow_migrations_lock_test'));

        $second->release('public.flow_migrations_lock_test');
    }

    public function test_second_session_acquires_after_release(): void
    {
        $first = new AdvisoryMigrationLock($this->context->client);
        $second = new AdvisoryMigrationLock($this->context->secondClient());

        static::assertTrue($first->acquire('public.flow_migrations'));

        $first->release('public.flow_migrations');

        static::assertTrue($second->acquire('public.flow_migrations'));

        $second->release('public.flow_migrations');
    }

    public function test_second_session_cannot_acquire_while_first_holds(): void
    {
        $first = new AdvisoryMigrationLock($this->context->client);
        $second = new AdvisoryMigrationLock($this->context->secondClient());

        static::assertTrue($first->acquire('public.flow_migrations'));
        static::assertFalse($second->acquire('public.flow_migrations'));

        $first->release('public.flow_migrations');
    }
}
