<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Executor;

use Flow\PostgreSql\Migrations\{Direction, MigrationContext, MigrationPlan, Version};
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Executor\{DefaultMigrationExecutor, ExecutionResult};
use Flow\PostgreSql\Migrations\Tests\Double\{FailingMigration, NonTransactionalMigration, NonTransactionalRollback, SpyClient, SpyMigration, SpyRollback};
use PHPUnit\Framework\TestCase;

final class DefaultMigrationExecutorTest extends TestCase
{
    public function test_execute_captures_exception() : void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new FailingMigration(),
            null,
            Direction::UP,
        );

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        self::assertFalse($result->isSuccessful());
        self::assertInstanceOf(\RuntimeException::class, $result->error);
        self::assertSame('Migration failed', $result->error->getMessage());
    }

    public function test_execute_down_calls_rollback() : void
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

        self::assertTrue($rollback->rollbackCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(Direction::DOWN, $result->direction);
    }

    public function test_execute_down_calls_rollback_in_transaction_when_transactional() : void
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

        self::assertTrue($rollback->rollbackCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(1, $client->transactionCallCount);
    }

    public function test_execute_down_calls_rollback_without_transaction_when_not_transactional() : void
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

        self::assertTrue($rollback->rollbackCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(0, $client->transactionCallCount);
    }

    public function test_execute_down_without_rollback_returns_error() : void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new SpyMigration(),
            null,
            Direction::DOWN,
        );

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        self::assertFalse($result->isSuccessful());
        self::assertInstanceOf(MigrationException::class, $result->error);
    }

    public function test_execute_up_calls_migrate() : void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new SpyMigration();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            $migration,
            null,
            Direction::UP,
        );

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        self::assertTrue($migration->migrateCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(Direction::UP, $result->direction);
    }

    public function test_execute_up_calls_migrate_in_transaction_when_transactional() : void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new SpyMigration();
        $client = new SpyClient();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            $migration,
            null,
            Direction::UP,
        );

        $result = $executor->execute($plan, new MigrationContext($client));

        self::assertTrue($migration->migrateCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(1, $client->transactionCallCount);
    }

    public function test_execute_up_calls_migrate_without_transaction_when_not_transactional() : void
    {
        $executor = new DefaultMigrationExecutor();
        $migration = new NonTransactionalMigration();
        $client = new SpyClient();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            $migration,
            null,
            Direction::UP,
        );

        $result = $executor->execute($plan, new MigrationContext($client));

        self::assertTrue($migration->migrateCalled);
        self::assertTrue($result->isSuccessful());
        self::assertSame(0, $client->transactionCallCount);
    }

    public function test_execution_time_is_recorded() : void
    {
        $executor = new DefaultMigrationExecutor();
        $plan = new MigrationPlan(
            Version::fromString('20260401120000'),
            new SpyMigration(),
            null,
            Direction::UP,
        );

        $result = $executor->execute($plan, new MigrationContext(new SpyClient()));

        self::assertGreaterThanOrEqual(0, $result->executionTimeMs);
    }

    public function test_failed_result_is_not_successful() : void
    {
        $result = new ExecutionResult(
            Version::fromString('20260401120000'),
            Direction::UP,
            0,
            false,
            new \RuntimeException('error'),
        );

        self::assertFalse($result->isSuccessful());
    }

    public function test_skipped_result_is_not_successful() : void
    {
        $result = new ExecutionResult(
            Version::fromString('20260401120000'),
            Direction::UP,
            0,
            true,
            null,
        );

        self::assertFalse($result->isSuccessful());
    }

    public function test_successful_result_is_successful() : void
    {
        $result = new ExecutionResult(
            Version::fromString('20260401120000'),
            Direction::UP,
            0,
            false,
            null,
        );

        self::assertTrue($result->isSuccessful());
    }
}
