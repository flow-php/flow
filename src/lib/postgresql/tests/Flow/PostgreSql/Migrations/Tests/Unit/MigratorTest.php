<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Exception\MigrationLockedException;
use Flow\PostgreSql\Migrations\MigrationMode;
use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Context\MigratorContext;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Tests\Double\SpyRollback;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;
use stdClass;

use function array_filter;
use function iterator_to_array;

final class MigratorTest extends TestCase
{
    protected MigratorContext $migratorContext;

    protected function setUp(): void
    {
        $this->migratorContext = new MigratorContext();
    }

    public function test_execute_version_down(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);

        $result = $this->migratorContext->migrator([
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'first',
                new SpyMigration(),
                new SpyRollback(),
            ),
        ])->executeVersion(Version::fromString('20260401120000'), Direction::DOWN);

        static::assertTrue($result->isSuccessful());
        static::assertSame(Direction::DOWN, $result->direction);
        static::assertFalse(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260401120000')),
        );
    }

    public function test_execute_version_up(): void
    {
        $result = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
        ])->executeVersion(Version::fromString('20260401120000'), Direction::UP);

        static::assertTrue($result->isSuccessful());
        static::assertCount(1, $this->migratorContext->executor->executedPlans);
        static::assertTrue(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260401120000')),
        );
    }

    public function test_migrate_dry_run_does_not_store(): void
    {
        $results = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
        ])->migrate(dryRun: true);

        static::assertCount(1, $results);
        static::assertTrue($results[0]->isSuccessful());
        static::assertCount(0, $this->migratorContext->store->executedMigrations());
    }

    public function test_migrate_executes_pending_migrations(): void
    {
        $results = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
        ])->migrate();

        static::assertCount(3, $results);
        static::assertCount(3, $this->migratorContext->executor->executedPlans);

        foreach ($results as $result) {
            static::assertTrue($result->isSuccessful());
            static::assertSame(Direction::UP, $result->direction);
        }
    }

    public function test_migrate_executes_pending_migration_older_than_latest_executed(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'older', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'newer', new SpyMigration(), null),
        ]);

        $results = $migrator->migrate();

        static::assertCount(1, $results);
        static::assertSame(Direction::UP, $results[0]->direction);
        static::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
        static::assertCount(0, $migrator->status()->pending());
    }

    public function test_migrate_executes_pending_migrations_around_latest_executed_in_version_order(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'older', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'executed', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'newer', new SpyMigration(), null),
        ]);

        $results = $migrator->migrate();

        static::assertCount(2, $results);
        static::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
        static::assertTrue($results[1]->version->equals(Version::fromString('20260403120000')));
        static::assertCount(0, $migrator->status()->pending());
    }

    public function test_migrate_to_latest_executed_version_executes_older_pending_migration_only(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);

        $results = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'older', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'executed', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'newer', new SpyMigration(), null),
        ])->migrate(Version::fromString('20260402120000'));

        static::assertCount(1, $results);
        static::assertSame(Direction::UP, $results[0]->direction);
        static::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
        static::assertFalse(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260403120000')),
        );
    }

    public function test_migrate_in_strict_order_executes_all_migrations_when_nothing_is_executed(): void
    {
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
        ], mode: MigrationMode::STRICT_ORDER);

        static::assertCount(2, $migrator->migrate());
        static::assertCount(2, $this->migratorContext->store->executedMigrations());
    }

    public function test_migrate_in_strict_order_fails_on_pending_migrations_older_than_latest_executed(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260403120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'oldest', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'older', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'executed', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260404120000'), 'newer', new SpyMigration(), null),
        ], mode: MigrationMode::STRICT_ORDER);

        try {
            $migrator->migrate();
            static::fail('Expected MigrationException');
        } catch (MigrationException $exception) {
            static::assertStringContainsString(
                '"20260401120000", "20260402120000" are older than the latest executed migration "20260403120000"',
                $exception->getMessage(),
            );
        }

        static::assertSame([], $this->migratorContext->executor->executedPlans);
        static::assertCount(1, $this->migratorContext->store->executedMigrations());
    }

    public function test_migrate_in_strict_order_executes_pending_migrations_newer_than_latest_executed(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'executed', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'newer', new SpyMigration(), null),
        ], mode: MigrationMode::STRICT_ORDER);

        static::assertCount(1, $migrator->migrate());
        static::assertCount(2, $this->migratorContext->store->executedMigrations());
    }

    public function test_migrate_in_strict_order_rolls_back_when_older_migration_is_pending(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);
        $this->migratorContext->store->complete(Version::fromString('20260403120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'older', new SpyMigration(), null),
            new AvailableMigration(
                Version::fromString('20260402120000'),
                'target',
                new SpyMigration(),
                new SpyRollback(),
            ),
            new AvailableMigration(
                Version::fromString('20260403120000'),
                'latest',
                new SpyMigration(),
                new SpyRollback(),
            ),
        ], mode: MigrationMode::STRICT_ORDER);

        $results = $migrator->migrate(Version::fromString('20260402120000'));

        static::assertCount(1, $results);
        static::assertSame(Direction::DOWN, $results[0]->direction);
        static::assertTrue($results[0]->version->equals(Version::fromString('20260403120000')));
    }

    public function test_migrate_propagates_configuration_attributes_to_context(): void
    {
        $attribute = new stdClass();

        $this->migratorContext
            ->migrator([
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ], attributes: ['container' => $attribute])
            ->migrate();

        static::assertCount(1, $this->migratorContext->executor->executedContexts);
        static::assertSame($attribute, $this->migratorContext->executor->executedContexts[0]->attribute('container'));
    }

    public function test_migrate_initializes_store(): void
    {
        $this->migratorContext->migrator()->migrate();

        static::assertTrue($this->migratorContext->store->isInitialized());
    }

    public function test_status_does_not_initialize_store(): void
    {
        $status = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
        ])->status();

        static::assertFalse($this->migratorContext->store->isInitialized());
        static::assertCount(1, $status->pending());
        static::assertCount(0, $status->executed());
    }

    public function test_migrate_overrides_configuration_all_or_nothing(): void
    {
        $this->migratorContext
            ->migrator([
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ], allOrNothing: true)
            ->migrate(allOrNothing: false);

        static::assertSame(0, $this->migratorContext->client->transactionCallCount);
    }

    public function test_migrate_removes_on_down(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);
        $this->migratorContext->store->complete(Version::fromString('20260403120000'), 100);
        $migrator = $this->migratorContext->migrator([
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'first',
                new SpyMigration(),
                new SpyRollback(),
            ),
            new AvailableMigration(
                Version::fromString('20260402120000'),
                'second',
                new SpyMigration(),
                new SpyRollback(),
            ),
            new AvailableMigration(
                Version::fromString('20260403120000'),
                'third',
                new SpyMigration(),
                new SpyRollback(),
            ),
        ]);

        $results = $migrator->migrate(Version::fromString('20260401120000'));

        static::assertCount(2, $results);
        static::assertCount(1, $this->migratorContext->store->executedMigrations());
        static::assertTrue(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260401120000')),
        );
    }

    public function test_migrate_returns_empty_when_already_at_target(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);

        static::assertSame(
            [],
            $this->migratorContext->migrator([
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ])->migrate(Version::fromString('20260401120000')),
        );
    }

    public function test_migrate_returns_empty_when_no_available(): void
    {
        static::assertSame([], $this->migratorContext->migrator()->migrate());
    }

    public function test_migrate_stores_completed_on_success(): void
    {
        $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
        ])->migrate();

        static::assertCount(2, $this->migratorContext->store->executedMigrations());
        static::assertTrue(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260401120000')),
        );
        static::assertTrue(
            $this->migratorContext->store->executedMigrations()->has(Version::fromString('20260402120000')),
        );
    }

    public function test_migrate_up_to_specific_version(): void
    {
        $results = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
        ])->migrate(Version::fromString('20260402120000'));

        static::assertCount(2, $results);
        static::assertCount(2, $this->migratorContext->executor->executedPlans);
    }

    public function test_migrate_uses_all_or_nothing_from_configuration(): void
    {
        $this->migratorContext
            ->migrator([
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ], allOrNothing: true)
            ->migrate();

        static::assertSame(1, $this->migratorContext->client->transactionCallCount);
    }

    public function test_migrate_without_transaction_when_all_or_nothing_disabled(): void
    {
        $this->migratorContext
            ->migrator([
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ], allOrNothing: false)
            ->migrate();

        static::assertSame(0, $this->migratorContext->client->transactionCallCount);
    }

    public function test_status_shows_pending_and_executed(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);

        $status = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
        ])->status();

        static::assertCount(2, $status);
        static::assertCount(1, $status->executed());
        static::assertCount(1, $status->pending());
    }

    public function test_status_shows_unavailable(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);
        $this->migratorContext->store->complete(Version::fromString('20260499120000'), 100);

        $status = $this->migratorContext->migrator([
            new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
        ])->status();

        static::assertCount(2, $status);
        static::assertCount(1, $status->executed());

        $items = iterator_to_array($status);
        $unavailable = array_filter($items, static fn($s) => $s->state === MigrationState::UNAVAILABLE);

        static::assertCount(1, $unavailable);
    }

    public function test_execute_version_acquires_and_releases_the_lock(): void
    {
        $this->migratorContext->migrator([
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'first',
                new SpyMigration(),
                new SpyRollback(),
            ),
        ], lock: $this->migratorContext->lock)->executeVersion(Version::fromString('20260401120000'), Direction::UP);

        static::assertSame(
            [
                ['acquire', 'testdb.public.flow_migrations'],
                ['release', 'testdb.public.flow_migrations'],
            ],
            $this->migratorContext->lock->calls,
        );
        static::assertCount(1, $this->migratorContext->store->executedMigrations());
    }

    public function test_execute_version_down_refuses_a_version_that_is_not_executed(): void
    {
        try {
            $this->migratorContext->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)->executeVersion(
                Version::fromString('20260401120000'),
                Direction::DOWN,
            );
            static::fail('Expected MigrationException');
        } catch (MigrationException $exception) {
            static::assertSame(
                'Migration "20260401120000" is not executed, there is nothing to roll back.',
                $exception->getMessage(),
            );
        }

        static::assertSame([], $this->migratorContext->executor->executedPlans);
        static::assertSame(
            [
                ['acquire', 'testdb.public.flow_migrations'],
                ['release', 'testdb.public.flow_migrations'],
            ],
            $this->migratorContext->lock->calls,
        );
    }

    public function test_execute_version_down_on_an_unknown_version_reports_it_as_not_found(): void
    {
        $this->expectExceptionObject(MigrationException::versionNotFound(Version::fromString('20260401120000')));

        $this->migratorContext->migrator()->executeVersion(Version::fromString('20260401120000'), Direction::DOWN);
    }

    public function test_execute_version_dry_run_down_refuses_a_version_that_is_not_executed(): void
    {
        $this->expectExceptionObject(MigrationException::notExecuted(Version::fromString('20260401120000')));

        try {
            $this->migratorContext->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)->executeVersion(
                Version::fromString('20260401120000'),
                Direction::DOWN,
                dryRun: true,
            );
        } finally {
            static::assertSame([], $this->migratorContext->executor->executedPlans);
            static::assertSame([], $this->migratorContext->lock->calls);
        }
    }

    public function test_execute_version_dry_run_takes_no_lock(): void
    {
        $this->migratorContext->migrator([
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'first',
                new SpyMigration(),
                new SpyRollback(),
            ),
        ], lock: $this->migratorContext->lock)->executeVersion(
            Version::fromString('20260401120000'),
            Direction::UP,
            dryRun: true,
        );

        static::assertCount(1, $this->migratorContext->executor->executedPlans);
        static::assertSame([], $this->migratorContext->lock->calls);
    }

    public function test_execute_version_dry_run_up_refuses_an_executed_version(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);

        $this->expectExceptionObject(MigrationException::alreadyExecuted(Version::fromString('20260401120000')));

        try {
            $this->migratorContext->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)->executeVersion(
                Version::fromString('20260401120000'),
                Direction::UP,
                dryRun: true,
            );
        } finally {
            static::assertSame([], $this->migratorContext->executor->executedPlans);
            static::assertSame([], $this->migratorContext->lock->calls);
        }
    }

    public function test_execute_version_throws_when_the_lock_is_busy(): void
    {
        $this->migratorContext->lock->acquired = false;

        $this->expectExceptionObject(new MigrationLockedException('testdb.public.flow_migrations'));

        try {
            $this->migratorContext->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)->executeVersion(
                Version::fromString('20260401120000'),
                Direction::UP,
            );
        } finally {
            static::assertFalse($this->migratorContext->store->isInitialized());
            static::assertSame([], $this->migratorContext->executor->executedPlans);
            static::assertSame([['acquire', 'testdb.public.flow_migrations']], $this->migratorContext->lock->calls);
        }
    }

    public function test_execute_version_up_refuses_an_executed_version(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260401120000'), 100);

        try {
            $this->migratorContext->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)->executeVersion(
                Version::fromString('20260401120000'),
                Direction::UP,
            );
            static::fail('Expected MigrationException');
        } catch (MigrationException $exception) {
            static::assertSame(
                'Migration "20260401120000" is already executed. Roll it back first to execute it again.',
                $exception->getMessage(),
            );
        }

        static::assertSame([], $this->migratorContext->executor->executedPlans);
        static::assertSame(
            [
                ['acquire', 'testdb.public.flow_migrations'],
                ['release', 'testdb.public.flow_migrations'],
            ],
            $this->migratorContext->lock->calls,
        );
    }

    public function test_lock_name_is_database_schema_and_table(): void
    {
        $this->migratorContext
            ->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)
            ->migrate();

        static::assertSame('testdb.public.flow_migrations', $this->migratorContext->lock->calls[0][1]);
    }

    public function test_migrate_acquires_and_releases_the_lock(): void
    {
        $results = $this->migratorContext
            ->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)
            ->migrate();

        static::assertCount(1, $results);
        static::assertSame(
            [
                ['acquire', 'testdb.public.flow_migrations'],
                ['release', 'testdb.public.flow_migrations'],
            ],
            $this->migratorContext->lock->calls,
        );
    }

    public function test_migrate_dry_run_takes_no_lock(): void
    {
        $this->migratorContext
            ->migrator([
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ], lock: $this->migratorContext->lock)
            ->migrate(dryRun: true);

        static::assertCount(1, $this->migratorContext->executor->executedPlans);
        static::assertSame([], $this->migratorContext->lock->calls);
    }

    public function test_migrate_propagates_a_release_failure_after_a_successful_run(): void
    {
        $this->migratorContext->lock->releaseFailure = MigrationException::advisoryLockNotHeld(
            'testdb.public.flow_migrations',
        );

        $this->expectExceptionObject(MigrationException::advisoryLockNotHeld('testdb.public.flow_migrations'));

        try {
            $this->migratorContext
                ->migrator([
                    new AvailableMigration(
                        Version::fromString('20260401120000'),
                        'first',
                        new SpyMigration(),
                        new SpyRollback(),
                    ),
                ], lock: $this->migratorContext->lock)
                ->migrate();
        } finally {
            static::assertCount(1, $this->migratorContext->store->executedMigrations());
        }
    }

    public function test_migrate_releases_the_lock_and_rethrows_when_the_run_fails(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);

        $this->expectExceptionObject(MigrationException::outOfOrder(
            Version::fromString('20260402120000'),
            Version::fromString('20260401120000'),
        ));

        try {
            $this->migratorContext
                ->migrator(
                    [
                        new AvailableMigration(
                            Version::fromString('20260401120000'),
                            'older',
                            new SpyMigration(),
                            null,
                        ),
                        new AvailableMigration(
                            Version::fromString('20260402120000'),
                            'executed',
                            new SpyMigration(),
                            null,
                        ),
                    ],
                    mode: MigrationMode::STRICT_ORDER,
                    lock: $this->migratorContext->lock,
                )
                ->migrate();
        } finally {
            static::assertSame(
                [
                    ['acquire', 'testdb.public.flow_migrations'],
                    ['release', 'testdb.public.flow_migrations'],
                ],
                $this->migratorContext->lock->calls,
            );
        }
    }

    public function test_migrate_release_failure_after_a_failed_run_keeps_the_original_exception(): void
    {
        $this->migratorContext->store->complete(Version::fromString('20260402120000'), 100);
        $this->migratorContext->lock->releaseFailure = MigrationException::advisoryLockNotHeld(
            'testdb.public.flow_migrations',
        );

        $this->expectExceptionObject(MigrationException::outOfOrder(
            Version::fromString('20260402120000'),
            Version::fromString('20260401120000'),
        ));

        try {
            $this->migratorContext
                ->migrator(
                    [
                        new AvailableMigration(
                            Version::fromString('20260401120000'),
                            'older',
                            new SpyMigration(),
                            null,
                        ),
                        new AvailableMigration(
                            Version::fromString('20260402120000'),
                            'executed',
                            new SpyMigration(),
                            null,
                        ),
                    ],
                    mode: MigrationMode::STRICT_ORDER,
                    lock: $this->migratorContext->lock,
                )
                ->migrate();
        } finally {
            static::assertSame(
                [
                    ['acquire', 'testdb.public.flow_migrations'],
                    ['release', 'testdb.public.flow_migrations'],
                ],
                $this->migratorContext->lock->calls,
            );
        }
    }

    public function test_migrate_throws_when_the_lock_is_busy(): void
    {
        $this->migratorContext->lock->acquired = false;

        $this->expectExceptionObject(new MigrationLockedException('testdb.public.flow_migrations'));

        try {
            $this->migratorContext
                ->migrator([
                    new AvailableMigration(
                        Version::fromString('20260401120000'),
                        'first',
                        new SpyMigration(),
                        new SpyRollback(),
                    ),
                ], lock: $this->migratorContext->lock)
                ->migrate();
        } finally {
            static::assertFalse($this->migratorContext->store->isInitialized());
            static::assertSame([], $this->migratorContext->executor->executedPlans);
            static::assertSame([['acquire', 'testdb.public.flow_migrations']], $this->migratorContext->lock->calls);
        }
    }
}
