<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function max;

final class ChainedBatchSizesTest extends FlowTestCase
{
    public function test_each_chained_source_keeps_its_own_batch_size(): void
    {
        $memory = memory_filesystem();

        foreach (['first' => 100, 'second' => 1_000] as $name => $count) {
            df()
                ->read(from_sequence_number('id', 1, $count))
                ->write(to_floe(path("memory://chained/{$name}.floe"), filesystem: $memory))
                ->run();
        }

        $first = from_floe(path('memory://chained/first.floe'), filesystem: $memory);
        $second = from_floe(path('memory://chained/second.floe'), filesystem: $memory);

        $firstRows = 0;

        foreach ($first->extract(flow_context()->backend(), new Criteria()) as $rows) {
            $firstRows += $rows->count();
        }

        $read = 0;
        $largestFromSecond = 0;

        foreach (from_all($first->withBatchSize(10), $second->withBatchSize(500))->extract(
            flow_context()->backend(),
            new Criteria(),
        ) as $rows) {
            if ($read < $firstRows) {
                static::assertLessThanOrEqual(10, $rows->count());
            } else {
                static::assertLessThanOrEqual(500, $rows->count());
                $largestFromSecond = max($largestFromSecond, $rows->count());
            }

            $read += $rows->count();
        }

        // above the first child's 10: the second child batched at its own size, not at the first one's
        static::assertGreaterThan(10, $largestFromSecond);
    }
}
