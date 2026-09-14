<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use DomainException;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\TransactionRolledBack;
use Flow\ETL\Pipeline\SideOffers;
use Flow\ETL\Pipeline\TransactionalSinks;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\SideRootMother;
use Flow\ETL\Transformer\LimitTransformer;
use RuntimeException;

final class TransactionalSinksTest extends FlowTestCase
{
    public function test_one_transaction_per_batch(): void
    {
        $transaction = new RecordingTransaction();
        $bare = new RecordingLoader();
        $side = new RecordingLoader();
        $sinks = new TransactionalSinks($transaction, [
            $bare,
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $side),
        ]);

        $sinks->load(SideRootMother::batch(1), NodeMother::context());
        $sinks->load(SideRootMother::batch(2), NodeMother::context());

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame(['load#1(1)', 'load#2(1)'], $bare->log);
        static::assertSame(['load#1(1)', 'load#2(1)'], $side->log);
    }

    public function test_closure_wraps_every_child_drain_in_one_transaction(): void
    {
        $transaction = new RecordingTransaction();
        $bare = new RecordingLoader();
        $side = new RecordingLoader();
        $sinks = new TransactionalSinks($transaction, [
            $bare,
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $side),
        ]);
        $sinks->load(SideRootMother::batch(), NodeMother::context());

        $sinks->closure(NodeMother::context());

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame(['load#1(1)', 'closure'], $bare->log);
        static::assertSame(['load#1(1)', 'closure'], $side->log);
    }

    public function test_a_child_failure_rolls_back_and_throws_transaction_rolled_back(): void
    {
        $boom = new RuntimeException('boom');
        $transaction = new RecordingTransaction();
        $failing = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), new RecordingLoader($boom));
        $sinks = new TransactionalSinks($transaction, [new RecordingLoader(), $failing]);

        try {
            $sinks->load(SideRootMother::batch(), NodeMother::context());
            static::fail('load() must surface the rolled back child failure');
        } catch (TransactionRolledBack $rolledBack) {
            static::assertSame($failing, $rolledBack->loader);
            static::assertSame($boom, $rolledBack->cause);
        }

        static::assertSame(['begin', 'rollback'], $transaction->log);
    }

    public function test_a_bare_child_failure_is_reported_against_that_child(): void
    {
        $boom = new RuntimeException('boom');
        $failing = new RecordingLoader($boom);

        try {
            (new TransactionalSinks(new RecordingTransaction(), [$failing]))->load(
                SideRootMother::batch(),
                NodeMother::context(),
            );
            static::fail('load() must surface the rolled back child failure');
        } catch (TransactionRolledBack $rolledBack) {
            static::assertSame($failing, $rolledBack->loader);
            static::assertSame($boom, $rolledBack->cause);
        }
    }

    public function test_a_rolled_back_child_writes_the_next_batch(): void
    {
        $transaction = new RecordingTransaction();
        $side = new RecordingLoader(new RuntimeException('boom'));
        $sinks = new TransactionalSinks($transaction, [SideRootMother::sinkFeed(
            new SideOffers(new ThrowError()),
            $side,
        )]);

        try {
            $sinks->load(SideRootMother::batch(1), NodeMother::context());
        } catch (TransactionRolledBack) {
            $sinks->load(SideRootMother::batch(2), NodeMother::context());
        }

        static::assertSame(['load#1 THROW', 'discard', 'load#2(1)'], $side->log);
        static::assertSame(['begin', 'rollback', 'begin', 'commit'], $transaction->log);
    }

    public function test_a_restart_keeps_the_child_s_limit_counter(): void
    {
        $side = new RecordingLoader(new RuntimeException('boom'), 2);
        $sinks = new TransactionalSinks(new RecordingTransaction(), [
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $side, new LimitTransformer(3)),
        ]);
        $sinks->load(SideRootMother::batch(1), NodeMother::context());

        try {
            $sinks->load(SideRootMother::batch(2), NodeMother::context());
        } catch (TransactionRolledBack) {
            $sinks->load(SideRootMother::batch(3), NodeMother::context());
            $sinks->load(SideRootMother::batch(4), NodeMother::context());
        }

        static::assertSame(['load#1(1)', 'load#2 THROW', 'discard', 'load#3(1)', 'closure'], $side->log);
    }

    public function test_a_live_sibling_is_not_restarted(): void
    {
        $live = new RecordingLoader();
        $sinks = new TransactionalSinks(new RecordingTransaction(), [
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $live),
            SideRootMother::sinkFeed(
                new SideOffers(new ThrowError()),
                new RecordingLoader(new RuntimeException('boom')),
            ),
        ]);

        try {
            $sinks->load(SideRootMother::batch(1), NodeMother::context());
        } catch (TransactionRolledBack) {
            $sinks->load(SideRootMother::batch(2), NodeMother::context());
        }

        static::assertSame(['load#1(1)', 'load#2(1)'], $live->log);
    }

    public function test_a_failing_commit_rolls_the_batch_back(): void
    {
        $failure = new RuntimeException('commit failed');
        $transaction = new RecordingTransaction(commitFailure: $failure);

        try {
            (new TransactionalSinks($transaction, [new RecordingLoader()]))->load(
                SideRootMother::batch(),
                NodeMother::context(),
            );
            static::fail('load() must rethrow the commit failure');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertSame(['begin', 'commit', 'rollback'], $transaction->log);
        static::assertSame([$failure], $transaction->rolledBackFor);
    }

    public function test_a_failing_commit_rolls_the_drain_back(): void
    {
        $failure = new RuntimeException('commit failed');
        $transaction = new RecordingTransaction(commitFailure: $failure);

        try {
            (new TransactionalSinks($transaction, [new RecordingLoader()]))->closure(NodeMother::context());
            static::fail('closure() must rethrow the commit failure');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertSame(['begin', 'commit', 'rollback'], $transaction->log);
    }

    public function test_a_failing_begin_does_not_roll_back(): void
    {
        $failure = new RuntimeException('begin failed');
        $transaction = new RecordingTransaction(beginFailure: $failure);
        $loader = new RecordingLoader();

        try {
            (new TransactionalSinks($transaction, [$loader]))->load(SideRootMother::batch(), NodeMother::context());
            static::fail('load() must rethrow the begin failure');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertSame(['begin'], $transaction->log);
        static::assertSame([], $loader->log);
    }

    public function test_a_rollback_failure_is_logged_and_does_not_mask_the_cause(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $boom = new RuntimeException('boom');

        try {
            (new TransactionalSinks(
                new RecordingTransaction(rollbackFailure: new RuntimeException('rollback failed')),
                [new RecordingLoader($boom)],
            ))->load(SideRootMother::batch(), $telemetry->flowContext);
            static::fail('load() must surface the child failure');
        } catch (TransactionRolledBack $rolledBack) {
            static::assertSame($boom, $rolledBack->cause);
        }

        static::assertCount(1, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_rollback_failure_on_a_failing_batch_commit_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('commit failed');

        try {
            (new TransactionalSinks(
                new RecordingTransaction(
                    commitFailure: $failure,
                    rollbackFailure: new RuntimeException('rollback failed'),
                ),
                [new RecordingLoader()],
            ))->load(SideRootMother::batch(), $telemetry->flowContext);
            static::fail('load() must rethrow the commit failure');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertCount(1, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_rollback_failure_on_a_failing_drain_commit_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('commit failed');

        try {
            (new TransactionalSinks(
                new RecordingTransaction(
                    commitFailure: $failure,
                    rollbackFailure: new RuntimeException('rollback failed'),
                ),
                [new RecordingLoader()],
            ))->closure($telemetry->flowContext);
            static::fail('closure() must rethrow the commit failure');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertCount(1, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_closure_failure_rethrows_the_users_class(): void
    {
        $drain = new DomainException('drain-boom');
        $transaction = new RecordingTransaction();

        try {
            (new TransactionalSinks($transaction, [
                SideRootMother::sinkFeed(new SideOffers(new ThrowError()), new RecordingLoader(closureFailure: $drain)),
            ]))->closure(NodeMother::context());
            static::fail('closure() must rethrow the drain failure');
        } catch (DomainException $thrown) {
            static::assertSame($drain, $thrown);
        }

        static::assertSame(['begin', 'rollback'], $transaction->log);
    }

    public function test_discard_forwards_to_every_child_without_a_transaction(): void
    {
        $transaction = new RecordingTransaction();
        $bare = new RecordingLoader();
        $side = new RecordingLoader();

        (new TransactionalSinks($transaction, [
            $bare,
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $side),
        ]))->discard(NodeMother::context());

        static::assertSame(['discard'], $bare->log);
        static::assertSame(['discard'], $side->log);
        static::assertSame([], $transaction->log);
    }
}
