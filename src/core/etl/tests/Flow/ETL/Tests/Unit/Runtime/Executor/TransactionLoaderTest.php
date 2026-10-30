<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use DomainException;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\Executor\OfferedFailure;
use Flow\ETL\Runtime\Executor\TransactionLoader;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BranchLoaderMother;
use Flow\ETL\Tests\Mother\NodeMother;
use RuntimeException;

final class TransactionLoaderTest extends FlowTestCase
{
    public function test_one_transaction_per_batch(): void
    {
        $transaction = new RecordingTransaction();
        $bare = new RecordingLoader();
        $side = new RecordingLoader();
        $transactional = new TransactionLoader(
            $transaction,
            [
                $bare,
                BranchLoaderMother::branchLoader(new OfferedFailure(null), $side),
            ],
            NodeMother::context(),
        );

        $transactional->load(BranchLoaderMother::batch(1));
        $transactional->load(BranchLoaderMother::batch(2));

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame(['load#1(1)', 'load#2(1)'], $bare->log);
        static::assertSame(['load#1(1)', 'load#2(1)'], $side->log);
    }

    public function test_closure_wraps_every_child_drain_in_one_transaction(): void
    {
        $transaction = new RecordingTransaction();
        $bare = new RecordingLoader();
        $side = new RecordingLoader();
        $transactional = new TransactionLoader(
            $transaction,
            [
                $bare,
                BranchLoaderMother::branchLoader(new OfferedFailure(null), $side),
            ],
            NodeMother::context(),
        );
        $transactional->load(BranchLoaderMother::batch());

        $transactional->closure();

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame(['load#1(1)', 'closure'], $bare->log);
        static::assertSame(['load#1(1)', 'closure'], $side->log);
    }

    public function test_a_child_failure_rolls_back_and_names_the_users_loader_inside_the_child(): void
    {
        $boom = new RuntimeException('boom');
        $transaction = new RecordingTransaction();
        $loader = new RecordingLoader($boom);
        $transactional = new TransactionLoader(
            $transaction,
            [new RecordingLoader(), BranchLoaderMother::branchLoader(new OfferedFailure(null), $loader)],
            NodeMother::context(),
        );

        $offered = $transactional->load(BranchLoaderMother::batch());

        static::assertInstanceOf(LoadingError::class, $offered);
        static::assertSame($loader, $offered->loader);
        static::assertSame($boom, $offered->cause);

        static::assertSame(['begin', 'rollback'], $transaction->log);
    }

    public function test_a_child_failure_rolls_back_with_its_cause(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $boom = new RuntimeException('boom');
        $transaction = new RecordingTransaction();

        (new TransactionLoader($transaction, [new RecordingLoader($boom)], $telemetry->flowContext))->load(
            BranchLoaderMother::batch(),
        );

        static::assertSame(['begin', 'rollback'], $transaction->log);
        static::assertSame([$boom], $transaction->rolledBackFor);
        static::assertCount(0, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_bare_child_failure_is_reported_against_that_child(): void
    {
        $boom = new RuntimeException('boom');
        $failing = new RecordingLoader($boom);

        $offered = (new TransactionLoader(new RecordingTransaction(), [$failing], NodeMother::context()))->load(
            BranchLoaderMother::batch(),
        );

        static::assertInstanceOf(LoadingError::class, $offered);
        static::assertSame($failing, $offered->loader);
        static::assertSame($boom, $offered->cause);
    }

    public function test_a_rolled_back_child_writes_the_next_batch(): void
    {
        $transaction = new RecordingTransaction();
        $side = new RecordingLoader(new RuntimeException('boom'));
        $transactional = new TransactionLoader(
            $transaction,
            [BranchLoaderMother::branchLoader(new OfferedFailure(null), $side)],
            NodeMother::context(),
        );

        static::assertNotNull($transactional->load(BranchLoaderMother::batch(1)));
        static::assertNull($transactional->load(BranchLoaderMother::batch(2)));
        static::assertSame(['load#1 THROW', 'discard', 'load#2(1)'], $side->log);
        static::assertSame(['begin', 'rollback', 'begin', 'commit'], $transaction->log);
    }

    public function test_a_restart_keeps_the_child_s_limit_counter(): void
    {
        $side = new RecordingLoader(new RuntimeException('boom'), 2);
        $transactional = new TransactionLoader(
            new RecordingTransaction(),
            [
                BranchLoaderMother::branchLoader(new OfferedFailure(null), $side, new LimitTransformer(3)),
            ],
            NodeMother::context(),
        );
        $transactional->load(BranchLoaderMother::batch(1));

        static::assertNotNull($transactional->load(BranchLoaderMother::batch(2)));

        $transactional->load(BranchLoaderMother::batch(3));
        $transactional->load(BranchLoaderMother::batch(4));

        static::assertSame(['load#1(1)', 'load#2 THROW', 'discard', 'load#3(1)', 'closure'], $side->log);
    }

    public function test_a_live_sibling_is_not_restarted(): void
    {
        $live = new RecordingLoader();
        $transactional = new TransactionLoader(
            new RecordingTransaction(),
            [
                BranchLoaderMother::branchLoader(new OfferedFailure(null), $live),
                BranchLoaderMother::branchLoader(
                    new OfferedFailure(null),
                    new RecordingLoader(new RuntimeException('boom')),
                ),
            ],
            NodeMother::context(),
        );

        static::assertNotNull($transactional->load(BranchLoaderMother::batch(1)));

        $transactional->load(BranchLoaderMother::batch(2));

        static::assertSame(['load#1(1)', 'load#2(1)'], $live->log);
    }

    public function test_a_branch_child_failure_no_user_step_raised_rolls_back_and_surfaces_as_it_is(): void
    {
        $drain = new DomainException('drain-boom');
        $transaction = new RecordingTransaction();
        $side = new RecordingLoader(closureFailure: $drain);
        $transactional = new TransactionLoader(
            $transaction,
            [BranchLoaderMother::branchLoader(new OfferedFailure(null), $side, new LimitTransformer(1))],
            NodeMother::context(),
        );

        try {
            $transactional->load(BranchLoaderMother::batch());
            static::fail('load() must surface the ending failure');
        } catch (DomainException $thrown) {
            static::assertSame($drain, $thrown);
        }

        static::assertSame(['begin', 'rollback'], $transaction->log);
    }

    public function test_a_failing_commit_rolls_the_batch_back(): void
    {
        $failure = new RuntimeException('commit failed');
        $transaction = new RecordingTransaction(commitFailure: $failure);

        try {
            (new TransactionLoader($transaction, [new RecordingLoader()], NodeMother::context()))->load(
                BranchLoaderMother::batch(),
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
            (new TransactionLoader($transaction, [new RecordingLoader()], NodeMother::context()))->closure();
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
            (new TransactionLoader($transaction, [$loader], NodeMother::context()))->load(BranchLoaderMother::batch());
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

        static::assertSame(
            $boom,
            (new TransactionLoader(
                new RecordingTransaction(rollbackFailure: new RuntimeException('rollback failed')),
                [new RecordingLoader($boom)],
                $telemetry->flowContext,
            ))->load(BranchLoaderMother::batch())?->cause,
        );

        static::assertCount(1, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_rollback_failure_on_a_failing_batch_commit_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('commit failed');

        try {
            (new TransactionLoader(
                new RecordingTransaction(
                    commitFailure: $failure,
                    rollbackFailure: new RuntimeException('rollback failed'),
                ),
                [new RecordingLoader()],
                $telemetry->flowContext,
            ))->load(BranchLoaderMother::batch());
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
            (new TransactionLoader(
                new RecordingTransaction(
                    commitFailure: $failure,
                    rollbackFailure: new RuntimeException('rollback failed'),
                ),
                [new RecordingLoader()],
                $telemetry->flowContext,
            ))->closure();
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
            (new TransactionLoader(
                $transaction,
                [
                    BranchLoaderMother::branchLoader(
                        new OfferedFailure(null),
                        new RecordingLoader(closureFailure: $drain),
                    ),
                ],
                NodeMother::context(),
            ))->closure();
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

        (new TransactionLoader(
            $transaction,
            [
                $bare,
                BranchLoaderMother::branchLoader(new OfferedFailure(null), $side),
            ],
            NodeMother::context(),
        ))->discard();

        static::assertSame(['discard'], $bare->log);
        static::assertSame(['discard'], $side->log);
        static::assertSame([], $transaction->log);
    }
}
