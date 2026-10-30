<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\Context\LoaderEndingContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;

final class DiscardableTest extends FlowTestCase
{
    public function test_a_run_abandoned_mid_stream_discards_the_loader(): void
    {
        $loader = new RecordingLoader();

        $rows = data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write($loader)
            ->get();

        $rows->current();
        unset($rows);
        gc_collect_cycles();

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }

    public function test_a_run_that_completes_closes_the_loader_and_never_discards_it(): void
    {
        $loader = new RecordingLoader();

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write($loader)
            ->run();

        static::assertSame(['load#1(2)', 'closure'], $loader->log);
    }

    public function test_a_run_that_throws_discards_the_loader_and_never_closes_it(): void
    {
        $loader = new RecordingLoader();

        LoaderEndingContext::failedRun($loader);

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }

    public function test_a_discard_that_throws_after_a_failed_closure_does_not_replace_the_closure_failure(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $closureFailure = new RuntimeException('closure failed');
        $loader = new ClosureThrowingLoader($closureFailure, new RuntimeException('discard failed'));

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun($loader, $telemetry->config));
        static::assertSame(1, $loader->discarded);
        static::assertCount(
            1,
            $telemetry->logs->entriesContaining('Loader failed to discard after its closure failed.'),
        );
    }

    public function test_a_discard_that_throws_after_a_failed_run_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $loader = new ClosureThrowingLoader(
            new RuntimeException('closure failed'),
            new RuntimeException('discard failed'),
        );

        LoaderEndingContext::failedRun($loader, $telemetry->config);

        static::assertSame(1, $loader->discarded);
        static::assertCount(1, $telemetry->logs->entriesContaining('Loader failed to end after a failed run.'));
    }

    public function test_a_loader_whose_closure_throws_is_discarded_and_the_failure_rethrown(): void
    {
        $closureFailure = new RuntimeException('closure failed');
        $loader = new ClosureThrowingLoader($closureFailure);

        static::assertSame($closureFailure, LoaderEndingContext::thrownByRun($loader));
        static::assertSame(1, $loader->discarded);
    }

    public function test_a_branch_loader_is_discarded_when_its_closure_throws(): void
    {
        $closureFailure = new RuntimeException('closure failed');
        $loader = new ClosureThrowingLoader($closureFailure);

        static::assertSame(
            $closureFailure,
            LoaderEndingContext::thrownByRun(to_branch(ref('id')->isNotNull(), $loader)),
        );
        static::assertSame(1, $loader->discarded);
    }

    public function test_a_branch_loader_is_discarded(): void
    {
        $loader = new RecordingLoader();

        LoaderEndingContext::failedRun(to_branch(ref('id')->isNotNull(), $loader));

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }

    public function test_a_transformation_branch_loader_is_discarded(): void
    {
        $loader = new RecordingLoader();

        LoaderEndingContext::failedRun(to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df),
            $loader,
        ));

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }

    public function test_a_loader_wrapped_twice_is_discarded_once(): void
    {
        $loader = new RecordingLoader();

        LoaderEndingContext::failedRun(to_branch(
            ref('id')->isNotNull(),
            to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df), $loader),
        ));

        static::assertSame(['load#1(1)', 'discard'], $loader->log);
    }
}
