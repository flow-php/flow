<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use DomainException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\Executor\BranchLoader;
use Flow\ETL\Runtime\Executor\OfferedFailure;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BranchLoaderMother;
use Flow\ETL\Tests\Mother\NodeMother;
use RuntimeException;

final class BranchLoaderTest extends FlowTestCase
{
    public function test_discard_after_a_completed_run_reaches_every_consumer(): void
    {
        $feed = BranchLoaderMother::feed();
        $offers = new OfferedFailure(new ThrowError());
        $first = new RecordingLoader();
        $second = new RecordingLoader();
        $branch = new BranchLoader(
            $feed,
            BranchLoaderMother::run($feed, NodeMother::context()->withErrorHandler($offers), $first, $second),
            $offers,
            $first,
            $second,
        );

        $branch->closure();
        $branch->discard();

        static::assertSame(['closure', 'discard'], $first->log);
        static::assertSame(['closure', 'discard'], $second->log);
    }

    public function test_a_feed_without_writes_is_refused(): void
    {
        $feed = BranchLoaderMother::feed();
        $offers = new OfferedFailure(new ThrowError());

        $this->expectExceptionObject(new InvalidArgumentException('At least one write must be provided'));

        new BranchLoader($feed, BranchLoaderMother::run($feed, NodeMother::context()), $offers);
    }

    public function test_a_batch_is_loaded_in_the_same_resume_that_fed_it(): void
    {
        $loader = new RecordingLoader();

        BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader)->load(
            BranchLoaderMother::batch(),
        );

        static::assertSame(['load#1(1)'], $loader->log);
    }

    public function test_closure_drains_and_closes_the_side_loader(): void
    {
        $loader = new RecordingLoader();
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader);

        $branch->load(BranchLoaderMother::batch());
        $branch->closure();

        static::assertSame(['load#1(1)', 'closure'], $loader->log);
    }

    public function test_closure_on_a_never_started_run_still_closes(): void
    {
        $loader = new RecordingLoader();

        BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader)->closure();

        static::assertSame(['closure'], $loader->log);
    }

    public function test_discard_never_drains(): void
    {
        $loader = new RecordingLoader();

        BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader)->discard();

        static::assertSame(['discard'], $loader->log);
        static::assertSame(0, $loader->loadsCount);
    }

    public function test_a_failure_a_handler_decided_inside_the_branch_surfaces_as_it_is(): void
    {
        $boom = new RuntimeException('boom');

        $this->expectExceptionObject($boom);

        BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), new RecordingLoader($boom))->load(
            BranchLoaderMother::batch(),
        );
    }

    public function test_a_failure_no_handler_decided_is_returned_against_the_users_loader(): void
    {
        $boom = new RuntimeException('boom');
        $loader = new RecordingLoader($boom);

        $error = BranchLoaderMother::branchLoader(new OfferedFailure(null), $loader)->load(BranchLoaderMother::batch());

        static::assertInstanceOf(LoadingError::class, $error);
        static::assertSame($loader, $error->loader);
        static::assertSame($boom, $error->cause);
    }

    public function test_a_failed_run_is_dropped_so_the_next_batch_runs_a_fresh_one(): void
    {
        $loader = new RecordingLoader(new RuntimeException('boom'), 1);
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(null), $loader);

        static::assertNotNull($branch->load(BranchLoaderMother::batch(1)));
        static::assertNull($branch->load(BranchLoaderMother::batch(2)));
        static::assertSame(['load#1 THROW', 'discard', 'load#2(1)'], $loader->log);
    }

    public function test_an_ending_failure_during_load_surfaces_as_it_is_and_was_never_offered(): void
    {
        $drain = new DomainException('drain-boom');
        $loader = new RecordingLoader(closureFailure: $drain);
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(null), $loader, new LimitTransformer(2));
        $branch->load(BranchLoaderMother::batch(1));

        try {
            $branch->load(BranchLoaderMother::batch(2));
            static::fail('the second load() completes the side root and must surface its closure failure');
        } catch (DomainException $failure) {
            static::assertSame($drain, $failure);
        }

        static::assertSame(['load#1(1)', 'load#2(1)', 'closure THROW', 'discard'], $loader->log);
    }

    public function test_closure_rethrows_the_users_class(): void
    {
        $drain = new RuntimeException('drain-boom');
        $branch = BranchLoaderMother::branchLoader(
            new OfferedFailure(new ThrowError()),
            new RecordingLoader(closureFailure: $drain),
        );
        $branch->load(BranchLoaderMother::batch());

        $this->expectExceptionObject($drain);

        $branch->closure();
    }

    public function test_discard_after_a_terminated_and_dropped_failed_run_does_not_discard_again(): void
    {
        $loader = new RecordingLoader(new RuntimeException('boom'), 2);
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader);
        $branch->load(BranchLoaderMother::batch(1));

        try {
            $branch->load(BranchLoaderMother::batch(2));
        } catch (RuntimeException) {
        }

        $branch->discard();

        static::assertSame(['load#1(1)', 'load#2 THROW', 'discard'], $loader->log);
    }

    public function test_discard_after_a_completed_run_reaches_the_loader(): void
    {
        $loader = new RecordingLoader();
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader);

        $branch->closure();
        $branch->discard();

        static::assertSame(['closure', 'discard'], $loader->log);
    }

    public function test_discard_after_a_completed_run_skips_a_loader_that_cannot_discard(): void
    {
        $loader = new SpyLoader();
        $branch = BranchLoaderMother::branchLoader(new OfferedFailure(new ThrowError()), $loader);

        $branch->closure();
        $branch->discard();

        static::assertSame(1, $loader->closureCount);
    }

    public function test_drop_runs_even_when_advance_throws(): void
    {
        $feed = BranchLoaderMother::feed();
        $offers = new OfferedFailure(new ThrowError());
        $drain = new RuntimeException('drain-boom');
        $loader = new RecordingLoader(closureFailure: $drain);
        $run = BranchLoaderMother::run($feed, NodeMother::context()->withErrorHandler($offers), $loader);
        $branch = new BranchLoader($feed, $run, $offers, $loader);

        try {
            $branch->closure();
        } catch (RuntimeException $failure) {
            static::assertSame($drain, $failure);
        }

        static::assertTrue($run->terminated());
        static::assertFalse($run->completed());

        // the failed fiber was dropped, so the next advance() runs a fresh one over the same pipeline
        try {
            $run->advance();
        } catch (RuntimeException $failure) {
            static::assertSame($drain, $failure);
        }

        static::assertSame(['closure THROW', 'discard', 'closure THROW', 'discard'], $loader->log);
    }
}
