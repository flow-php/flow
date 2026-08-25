<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\RecordingSink;
use Flow\ETL\Tests\FlowTestCase;
use Throwable;

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

        $this->failedRun($sink);

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_in_a_branching_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        $this->failedRun(to_branch(ref('id')->isNotNull(), $sink));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_in_a_retry_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        $this->failedRun(write_with_retries($sink));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_in_a_transformation_loader_is_discarded(): void
    {
        $sink = new RecordingSink();

        $this->failedRun(to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df),
            $sink,
        ));

        static::assertSame(1, $sink->discarded);
        static::assertSame(0, $sink->closed);
    }

    public function test_a_sink_wrapped_twice_is_discarded_once(): void
    {
        $sink = new RecordingSink();

        $this->failedRun(write_with_retries(to_branch(ref('id')->isNotNull(), $sink)));

        static::assertSame(1, $sink->discarded);
    }

    protected function failedRun(Loader $loader): void
    {
        try {
            data_frame()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->batchSize(1)
                ->map(function (Row $row): Row {
                    if ($row->valueOf('id') === 2) {
                        throw new RuntimeException('boom');
                    }

                    return $row;
                })
                ->write($loader)
                ->run();
        } catch (Throwable) {
            // the run is expected to fail; what matters is which ending the sink was given
        }
    }
}
