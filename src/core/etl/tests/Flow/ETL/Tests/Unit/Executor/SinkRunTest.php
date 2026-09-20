<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor;
use Flow\ETL\Executor\SinkRun;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\SinkFeedMother;
use RuntimeException;

final class SinkRunTest extends FlowTestCase
{
    public function test_a_never_started_run_is_neither_terminated_nor_completed(): void
    {
        $run = new SinkRun(
            SinkFeedMother::pipeline(SinkFeedMother::feed(), NodeMother::context(), new RecordingLoader()),
            new Executor(),
        );

        static::assertFalse($run->terminated());
        static::assertFalse($run->completed());
    }

    public function test_advance_starts_a_never_started_run(): void
    {
        $feed = SinkFeedMother::feed();
        $loader = new RecordingLoader();
        $run = new SinkRun(SinkFeedMother::pipeline($feed, NodeMother::context(), $loader), new Executor());

        $feed->feed(SinkFeedMother::batch());
        $run->advance();

        static::assertSame(['load#1(1)'], $loader->log);
        static::assertFalse($run->terminated());
    }

    public function test_advance_on_a_terminated_run_is_a_no_op(): void
    {
        $feed = SinkFeedMother::feed();
        $loader = new RecordingLoader();
        $run = new SinkRun(SinkFeedMother::pipeline($feed, NodeMother::context(), $loader), new Executor());

        $feed->finish();
        $run->advance();
        $run->advance();

        static::assertSame(['closure'], $loader->log);
        static::assertTrue($run->terminated());
        static::assertTrue($run->completed());
    }

    public function test_drop_unwinds_a_suspended_fiber_promptly(): void
    {
        $feed = SinkFeedMother::feed();
        $loader = new RecordingLoader();
        $run = new SinkRun(SinkFeedMother::pipeline($feed, NodeMother::context(), $loader), new Executor());
        $feed->feed(SinkFeedMother::batch());
        $run->advance();

        $run->drop();

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }

    public function test_a_dropped_terminated_run_stays_terminated_and_completed(): void
    {
        $feed = SinkFeedMother::feed();
        $run = new SinkRun(
            SinkFeedMother::pipeline($feed, NodeMother::context(), new RecordingLoader()),
            new Executor(),
        );
        $feed->finish();
        $run->advance();

        $run->drop();

        static::assertTrue($run->terminated());
        static::assertTrue($run->completed());
    }

    public function test_a_run_whose_advance_threw_is_terminated_but_not_completed(): void
    {
        $feed = SinkFeedMother::feed();
        $boom = new RuntimeException('boom');
        $run = new SinkRun(
            SinkFeedMother::pipeline($feed, NodeMother::context(), new RecordingLoader($boom)),
            new Executor(),
        );
        $feed->feed(SinkFeedMother::batch());

        try {
            $run->advance();
            static::fail('advance() must rethrow the sink pipeline failure');
        } catch (RuntimeException $failure) {
            static::assertSame($boom, $failure);
        }

        static::assertTrue($run->terminated());
        static::assertFalse($run->completed());

        $run->drop();

        static::assertTrue($run->terminated());
        static::assertFalse($run->completed());
    }

    public function test_the_next_advance_after_drop_runs_a_fresh_fiber_over_the_same_pipeline(): void
    {
        $feed = SinkFeedMother::feed();
        $transformer = new SpyTransformer();
        $loader = new RecordingLoader(new RuntimeException('boom'), 2);
        $run = new SinkRun(
            SinkFeedMother::pipeline($feed, NodeMother::context(), $transformer, $loader),
            new Executor(),
        );

        $feed->feed(SinkFeedMother::batch(1));
        $run->advance();
        $feed->feed(SinkFeedMother::batch(2));

        try {
            $run->advance();
        } catch (RuntimeException) {
            $run->drop();
        }

        $feed->feed(SinkFeedMother::batch(3));
        $run->advance();
        $feed->feed(SinkFeedMother::batch(4));
        $run->advance();
        $feed->finish();
        $run->advance();

        static::assertSame(['load#1(1)', 'load#2 THROW', 'discard', 'load#3(1)', 'load#4(1)', 'closure'], $loader->log);
        static::assertSame(4, $transformer->seen);
    }
}
