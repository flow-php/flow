<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\Context\LoaderEndingContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\RecordingSink;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\write_with_retries;

final class DiscardableTest extends FlowTestCase
{
    public function test_a_run_abandoned_mid_stream_discards_the_sink(): void
    {
        $sink = new RecordingSink();

        $rows = data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write($sink)
            ->get();

        $rows->current();
        unset($rows);
        gc_collect_cycles();

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_run_that_completes_closes_the_sink_and_never_discards_it(): void
    {
        $sink = new RecordingSink();

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write($sink)
            ->run();

        static::assertSame(1, $sink->closed);
        static::assertSame(0, $sink->discarded);
    }

    public function test_a_run_that_throws_discards_the_sink_and_never_closes_it(): void
    {
        $sink = new RecordingSink();

        LoaderEndingContext::failedRun($sink);

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_discard_that_throws_after_a_failed_closure_does_not_replace_the_closure_failure(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $closureFailure = new RuntimeException('closure failed');
        $sink = new ClosureThrowingLoader($closureFailure, new RuntimeException('discard failed'));

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun($sink, $telemetry->config));
        static::assertSame(1, $sink->discarded);
        static::assertCount(
            1,
            $telemetry->logs->entriesContaining('Loader failed to discard after its closure failed.'),
        );
    }

    public function test_a_discard_that_throws_after_a_failed_run_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $sink = new ClosureThrowingLoader(
            new RuntimeException('closure failed'),
            new RuntimeException('discard failed'),
        );

        LoaderEndingContext::failedRun($sink, $telemetry->config);

        static::assertSame(1, $sink->discarded);
        static::assertCount(1, $telemetry->logs->entriesContaining('Loader failed to end after a failed run.'));
    }

    public function test_a_sink_wrapped_in_a_retrying_loader_is_discarded_when_its_closure_throws(): void
    {
        $closureFailure = new RuntimeException('closure failed');
        $sink = new ClosureThrowingLoader($closureFailure);

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun(write_with_retries($sink)));
        static::assertSame(1, $sink->discarded);
    }

    public function test_a_sink_whose_closure_throws_is_discarded_and_the_failure_rethrown(): void
    {
        $closureFailure = new RuntimeException('closure failed');
        $sink = new ClosureThrowingLoader($closureFailure);

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun($sink));
        static::assertSame(1, $sink->discarded);
    }

    public function test_a_sink_wrapped_in_a_branching_loader_is_discarded_when_its_closure_throws(): void
    {
        $closureFailure = new RuntimeException('closure failed');
        $sink = new ClosureThrowingLoader($closureFailure);

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun(to_branch(ref('id')->isNotNull(), $sink)));
        static::assertSame(1, $sink->discarded);
    }

    public function test_a_sink_wrapped_in_a_branching_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        LoaderEndingContext::failedRun(to_branch(ref('id')->isNotNull(), $sink));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_in_a_retry_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        LoaderEndingContext::failedRun(write_with_retries($sink));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_in_a_transformation_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        LoaderEndingContext::failedRun(to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df),
            $sink,
        ));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_twice_is_discarded_once(): void
    {
        $sink = new RecordingSink();

        LoaderEndingContext::failedRun(write_with_retries(to_branch(ref('id')->isNotNull(), $sink)));

        static::assertSame(1, $sink->discarded);
    }
}
