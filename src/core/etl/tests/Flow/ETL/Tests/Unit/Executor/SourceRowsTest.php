<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Cardinality;
use Flow\ETL\Dataset\SourceStatistics;
use Flow\ETL\Executor\SourceRows;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class SourceRowsTest extends FlowTestCase
{
    public function test_every_batch_passes_through_unchanged_and_is_counted(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2], ['id' => 3]])->withBatchSize(2);
        $sources = new SourceRows();

        $batches = iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), false), false);

        static::assertEquals(iterator_to_array($extractor->extract(flow_context()), false), $batches);
        static::assertEquals(
            [new SourceStatistics('ArrayExtractor', new Statistics(Cardinality::exact(3)), 3, true)],
            $sources->statistics(),
        );
    }

    public function test_a_stop_reaches_the_source_and_ends_the_count(): void
    {
        $extractor = new CountingExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
        );
        $extractor->withBatchSize(1);
        $sources = new SourceRows();
        $counted = $sources->count($extractor, $extractor->extract(flow_context()), false);

        static::assertCount(1, $counted->current());
        $counted->send(Signal::STOP);

        static::assertFalse($counted->valid());
        static::assertSame(1, $extractor->batchesYielded);
        static::assertSame(1, $sources->statistics()[0]->rows);
        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_a_narrowed_read_is_not_complete(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]]);
        $sources = new SourceRows();

        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), true));

        static::assertSame(2, $sources->statistics()[0]->rows);
        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_one_narrowed_read_makes_the_source_incomplete(): void
    {
        $extractor = from_array([['id' => 1]]);
        $sources = new SourceRows();

        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), false));
        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), true));

        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_interleaved_reads_of_one_source_add_up(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]])->withBatchSize(1);
        $sources = new SourceRows();
        $first = $sources->count($extractor, $extractor->extract(flow_context()), false);
        $second = $sources->count($extractor, $extractor->extract(flow_context()), false);

        $first->current();
        iterator_to_array($second);
        static::assertFalse($sources->statistics()[0]->complete);

        $first->next();
        $first->next();

        static::assertSame(4, $sources->statistics()[0]->rows);
        static::assertTrue($sources->statistics()[0]->complete);
    }

    public function test_a_source_read_twice_is_listed_once_with_both_counts(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]]);
        $sources = new SourceRows();

        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), false));
        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), false));

        static::assertCount(1, $sources->statistics());
        static::assertSame(4, $sources->statistics()[0]->rows);
    }

    public function test_sources_are_listed_in_the_order_they_were_first_read(): void
    {
        $first = from_array([['id' => 1]]);
        $second = from_array([['id' => 1], ['id' => 2]]);
        $sources = new SourceRows();

        iterator_to_array($sources->count($second, $second->extract(flow_context()), false));
        iterator_to_array($sources->count($first, $first->extract(flow_context()), false));

        static::assertSame([2, 1], [$sources->statistics()[0]->rows, $sources->statistics()[1]->rows]);
    }

    public function test_a_source_that_yields_nothing_is_listed_with_zero_rows(): void
    {
        $extractor = from_array([]);
        $sources = new SourceRows();

        iterator_to_array($sources->count($extractor, $extractor->extract(flow_context()), false));

        static::assertSame(0, $sources->statistics()[0]->rows);
    }

    public function test_a_source_never_read_is_not_listed(): void
    {
        $extractor = from_array([['id' => 1]]);
        $sources = new SourceRows();

        $sources->count($extractor, $extractor->extract(flow_context()), false);

        static::assertSame([], $sources->statistics());
    }
}
