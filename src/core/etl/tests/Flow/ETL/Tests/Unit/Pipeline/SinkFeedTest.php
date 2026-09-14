<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use DomainException;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SideRootFailure;
use Flow\ETL\Pipeline\SideOffers;
use Flow\ETL\Pipeline\SideRun;
use Flow\ETL\Pipeline\SinkFeed;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\SideRootMother;
use Flow\ETL\Transformer\LimitTransformer;
use RuntimeException;

final class SinkFeedTest extends FlowTestCase
{
    public function test_discard_after_a_completed_run_reaches_every_consumer(): void
    {
        $feed = SideRootMother::feed();
        $offers = new SideOffers(new ThrowError());
        $first = new RecordingLoader();
        $second = new RecordingLoader();
        $sinkFeed = new SinkFeed(
            $feed,
            new SideRun(SideRootMother::pipeline(
                $feed,
                NodeMother::context()->withErrorHandler($offers),
                $first,
                $second,
            )),
            $offers,
            $first,
            $second,
        );

        $sinkFeed->closure(NodeMother::context());
        $sinkFeed->discard(NodeMother::context());

        static::assertSame(['closure', 'discard'], $first->log);
        static::assertSame(['closure', 'discard'], $second->log);
    }

    public function test_a_feed_without_consumers_is_refused(): void
    {
        $feed = SideRootMother::feed();
        $offers = new SideOffers(new ThrowError());

        $this->expectExceptionObject(new InvalidArgumentException('At least one consumer must be provided'));

        new SinkFeed($feed, new SideRun(SideRootMother::pipeline($feed, NodeMother::context())), $offers);
    }

    public function test_a_batch_is_loaded_in_the_same_resume_that_fed_it(): void
    {
        $loader = new RecordingLoader();

        SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader)->load(
            SideRootMother::batch(),
            NodeMother::context(),
        );

        static::assertSame(['load#1(1)'], $loader->log);
    }

    public function test_closure_drains_and_closes_the_side_loader(): void
    {
        $loader = new RecordingLoader();
        $sinkFeed = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader);

        $sinkFeed->load(SideRootMother::batch(), NodeMother::context());
        $sinkFeed->closure(NodeMother::context());

        static::assertSame(['load#1(1)', 'closure'], $loader->log);
    }

    public function test_closure_on_a_never_started_run_still_closes(): void
    {
        $loader = new RecordingLoader();

        SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader)->closure(NodeMother::context());

        static::assertSame(['closure'], $loader->log);
    }

    public function test_discard_never_drains(): void
    {
        $loader = new RecordingLoader();

        SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader)->discard(NodeMother::context());

        static::assertSame(['discard'], $loader->log);
        static::assertSame(0, $loader->loadsCount);
    }

    public function test_a_failure_the_side_pipeline_offered_arrives_as_side_root_failure(): void
    {
        $boom = new RuntimeException('boom');

        try {
            SideRootMother::sinkFeed(new SideOffers(new ThrowError()), new RecordingLoader($boom))->load(
                SideRootMother::batch(),
                NodeMother::context(),
            );
            static::fail('load() must surface the offered failure');
        } catch (SideRootFailure $failure) {
            static::assertSame($boom, $failure->cause);
        }
    }

    public function test_an_ending_failure_during_load_arrives_raw(): void
    {
        $drain = new DomainException('drain-boom');
        $loader = new RecordingLoader(closureFailure: $drain);
        $sinkFeed = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader, new LimitTransformer(2));
        $sinkFeed->load(SideRootMother::batch(1), NodeMother::context());

        try {
            $sinkFeed->load(SideRootMother::batch(2), NodeMother::context());
            static::fail('the second load() completes the side root and must surface its closure failure');
        } catch (DomainException $failure) {
            static::assertSame($drain, $failure);
        }

        static::assertSame(['load#1(1)', 'load#2(1)', 'closure THROW', 'discard'], $loader->log);
    }

    public function test_closure_rethrows_the_users_class(): void
    {
        $drain = new RuntimeException('drain-boom');
        $sinkFeed = SideRootMother::sinkFeed(
            new SideOffers(new ThrowError()),
            new RecordingLoader(closureFailure: $drain),
        );
        $sinkFeed->load(SideRootMother::batch(), NodeMother::context());

        $this->expectExceptionObject($drain);

        $sinkFeed->closure(NodeMother::context());
    }

    public function test_discard_after_a_terminated_and_dropped_failed_run_does_not_discard_again(): void
    {
        $loader = new RecordingLoader(new RuntimeException('boom'), 2);
        $sinkFeed = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader);
        $sinkFeed->load(SideRootMother::batch(1), NodeMother::context());

        try {
            $sinkFeed->load(SideRootMother::batch(2), NodeMother::context());
        } catch (SideRootFailure) {
            $sinkFeed->restart();
        }

        $sinkFeed->discard(NodeMother::context());

        static::assertSame(['load#1(1)', 'load#2 THROW', 'discard'], $loader->log);
    }

    public function test_discard_after_a_completed_run_reaches_the_loader(): void
    {
        $loader = new RecordingLoader();
        $sinkFeed = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader);

        $sinkFeed->closure(NodeMother::context());
        $sinkFeed->discard(NodeMother::context());

        static::assertSame(['closure', 'discard'], $loader->log);
    }

    public function test_discard_after_a_completed_run_skips_a_loader_that_cannot_discard(): void
    {
        $loader = new SpyLoader();
        $sinkFeed = SideRootMother::sinkFeed(new SideOffers(new ThrowError()), $loader);

        $sinkFeed->closure(NodeMother::context());
        $sinkFeed->discard(NodeMother::context());

        static::assertSame(1, $loader->closureCount);
    }

    public function test_drop_runs_even_when_advance_throws(): void
    {
        $feed = SideRootMother::feed();
        $offers = new SideOffers(new ThrowError());
        $drain = new RuntimeException('drain-boom');
        $loader = new RecordingLoader(closureFailure: $drain);
        $run = new SideRun(SideRootMother::pipeline($feed, NodeMother::context()->withErrorHandler($offers), $loader));
        $sinkFeed = new SinkFeed($feed, $run, $offers, $loader);

        try {
            $sinkFeed->closure(NodeMother::context());
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

    public function test_the_loader_sees_the_context_the_plan_gave_the_side_pipeline(): void
    {
        $offers = new SideOffers(new ThrowError());
        $loader = new RecordingLoader();

        SideRootMother::sinkFeed($offers, $loader)->load(SideRootMother::batch(), NodeMother::context());

        static::assertSame([$offers], $loader->handlers);
    }
}
