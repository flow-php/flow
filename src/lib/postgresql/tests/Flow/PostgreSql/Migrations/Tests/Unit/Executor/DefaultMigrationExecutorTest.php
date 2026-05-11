<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Executor;

use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Executor\DefaultMigrationExecutor;
use Flow\PostgreSql\Migrations\Executor\ExecutionResult;
use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\MigrationPlan;
use Flow\PostgreSql\Migrations\Tests\Double\FailingMigration;
use Flow\PostgreSql\Migrations\Tests\Double\NonTransactionalMigration;
use Flow\PostgreSql\Migrations\Tests\Double\NonTransactionalRollback;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Tests\Double\SpyRollback;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class DefaultMigrationExecutorTest extends TestCase
{
    public function test_execute_captures_exception(): void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), new FailingMigration(), null, Direction::UP);

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        static::assertFalse($result->isSuccessful());
        static::assertInstanceOf(\RuntimeException::class, $result->error);
        static::assertSame('Migration failed', $result->error->getMessage());
    }

    public function test_execute_down_calls_rollback(): void
    {
        $executor = new DefaultMigrationExecutor();
        $rollback = new SpyRollback();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new SpyMigration(),
            $rollback,
            Direction::DOWN,
        );

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        static::assertTrue($rollback->rollbackCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(Direction::DOWN, $result->direction);
    }

    public function test_execute_down_calls_rollback_in_transaction_when_transactional(): void
    {
        $executor = new DefaultMigrationExecutor();
        $rollback = new SpyRollback();
        $client = new SpyClient();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new SpyMigration(),
            $rollback,
            Direction::DOWN,
        );

        $result = $executor->execute($plan, new MigrationContext($client));

        static::assertTrue($rollback->rollbackCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(1, $client->transactionCallCount);
    }

    public function test_execute_down_calls_rollback_without_transaction_when_not_transactional(): void
    {
        $executor = new DefaultMigrationExecutor();
        $rollback = new NonTransactionalRollback();
        $client = new SpyClient();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new SpyMigration(),
            $rollback,
            Direction::DOWN,
        );

        $result = $executor->execute($plan, new MigrationContext($client));

        static::assertTrue($rollback->rollbackCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(0, $client->transactionCallCount);
    }

    public function test_execute_down_without_rollback_returns_error(): void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), new SpyMigration(), null, Direction::DOWN);

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        static::assertFalse($result->isSuccessful());
        static::assertInstanceOf(MigrationException::class, $result->error);
    }

    public function test_execute_up_calls_migrate(): void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new SpyMigration();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), $migration, null, Direction::UP);

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        static::assertTrue($migration->migrateCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(Direction::UP, $result->direction);
    }

    public function test_execute_up_calls_migrate_in_transaction_when_transactional(): void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new SpyMigration();
        $client = new SpyClient();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), $migration, null, Direction::UP);

        $result = $executor->execute($plan, new MigrationContext($client));

        static::assertTrue($migration->migrateCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(1, $client->transactionCallCount);
    }

    public function test_execute_up_calls_migrate_without_transaction_when_not_transactional(): void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new NonTransactionalMigration();
        $client = new SpyClient();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), $migration, null, Direction::UP);

        $result = $executor->execute($plan, new MigrationContext($client));

        static::assertTrue($migration->migrateCalled);
        static::assertTrue($result->isSuccessful());
        static::assertSame(0, $client->transactionCallCount);
    }

    public function test_execution_time_is_recorded(): void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(Version::fromString('20260401120000'), new SpyMigration(), null, Direction::UP);

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        static::assertGreaterThanOrEqual(0, $result->executionTimeMs);
    }

    public function test_failed_result_is_not_successful(): void
    {
        $result = new ExecutionResult(
            Version::fromString('20260401120000'),
            Direction::UP,
            0,
            false,
            new \RuntimeException('error'),
        );

        static::assertFalse($result->isSuccessful());
    }

    public function test_skipped_result_is_not_successful(): void
    {
        $result = new ExecutionResult(Version::fromString('20260401120000'), Direction::UP, 0, true, null);

        static::assertFalse($result->isSuccessful());
    }

    public function test_successful_result_is_successful(): void
    {
        $result = new ExecutionResult(Version::fromString('20260401120000'), Direction::UP, 0, false, null);

        static::assertTrue($result->isSuccessful());
    }
}
