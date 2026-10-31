<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Runtime\Executor\SourceCounts;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class SourceCountsTest extends FlowTestCase
{
    public function test_every_batch_passes_through_unchanged_and_is_counted(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2], ['id' => 3]])->withBatchSize(2);
        $sources = new SourceCounts();

        $batches = iterator_to_array(
            $sources->count(
                $extractor,
                FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
                false,
            ),
            false,
        );

        static::assertEquals(
            iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false),
            $batches,
        );
        static::assertEquals(
            [new SourceStatistics('ArrayExtractor', new Statistics(Cardinality::exact(3)), 3, true)],
            $sources->statistics(),
        );
    }

    public function test_an_empty_one_absorbs_every_read_with_what_its_source_declared(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2], ['id' => 3]]);
        $attempt = new SourceCounts();
        $sources = new SourceCounts();
        iterator_to_array($attempt->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));

        $sources->absorb($attempt);

        static::assertEquals(
            [new SourceStatistics('ArrayExtractor', new Statistics(Cardinality::exact(3)), 3, true)],
            $sources->statistics(),
        );
    }

    public function test_an_absorbed_read_of_a_source_already_counted_adds_its_rows(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]]);
        $attempt = new SourceCounts();
        $sources = new SourceCounts();
        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));
        iterator_to_array($attempt->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));

        $sources->absorb($attempt);

        static::assertCount(1, $sources->statistics());
        static::assertSame(4, $sources->statistics()[0]->rows);
        static::assertTrue($sources->statistics()[0]->complete);
    }

    public function test_an_absorbed_open_or_narrowed_read_leaves_the_source_incomplete(): void
    {
        $open = from_array([['id' => 1]]);
        $narrowed = from_array([['id' => 2]]);
        $attempt = new SourceCounts();
        $sources = new SourceCounts();
        $attempt->open($open, false)->counted(1);
        iterator_to_array($attempt->count(
            $narrowed,
            FlowTestCase::extracted($narrowed, flow_context()->backend(), new Criteria()),
            true,
        ));

        $sources->absorb($attempt);

        static::assertSame([1, 1], [$sources->statistics()[0]->rows, $sources->statistics()[1]->rows]);
        static::assertSame([false, false], [$sources->statistics()[0]->complete, $sources->statistics()[1]->complete]);
    }

    public function test_a_stop_reaches_the_source_and_ends_the_count(): void
    {
        $extractor = new CountingExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))),
        );
        $extractor->withBatchSize(1);
        $sources = new SourceCounts();
        $counted = $sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        );

        $batch = $counted->current();

        static::assertInstanceOf(Rows::class, $batch);
        static::assertCount(1, $batch);
        $counted->send(Signal::STOP);

        static::assertFalse($counted->valid());
        static::assertSame(1, $extractor->batchesYielded);
        static::assertSame(1, $sources->statistics()[0]->rows);
        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_a_narrowed_read_is_not_complete(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]]);
        $sources = new SourceCounts();

        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            true,
        ));

        static::assertSame(2, $sources->statistics()[0]->rows);
        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_one_narrowed_read_makes_the_source_incomplete(): void
    {
        $extractor = from_array([['id' => 1]]);
        $sources = new SourceCounts();

        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));
        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            true,
        ));

        static::assertFalse($sources->statistics()[0]->complete);
    }

    public function test_interleaved_reads_of_one_source_add_up(): void
    {
        $extractor = from_array([['id' => 1], ['id' => 2]])->withBatchSize(1);
        $sources = new SourceCounts();
        $first = $sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        );
        $second = $sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        );

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
        $sources = new SourceCounts();

        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));
        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));

        static::assertCount(1, $sources->statistics());
        static::assertSame(4, $sources->statistics()[0]->rows);
    }

    public function test_sources_are_listed_in_the_order_they_were_first_read(): void
    {
        $first = from_array([['id' => 1]]);
        $second = from_array([['id' => 1], ['id' => 2]]);
        $sources = new SourceCounts();

        iterator_to_array($sources->count(
            $second,
            FlowTestCase::extracted($second, flow_context()->backend(), new Criteria()),
            false,
        ));
        iterator_to_array($sources->count(
            $first,
            FlowTestCase::extracted($first, flow_context()->backend(), new Criteria()),
            false,
        ));

        static::assertSame([2, 1], [$sources->statistics()[0]->rows, $sources->statistics()[1]->rows]);
    }

    public function test_a_source_that_yields_nothing_is_listed_with_zero_rows(): void
    {
        $extractor = from_array([]);
        $sources = new SourceCounts();

        iterator_to_array($sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        ));

        static::assertSame(0, $sources->statistics()[0]->rows);
    }

    public function test_a_source_never_read_is_not_listed(): void
    {
        $extractor = from_array([['id' => 1]]);
        $sources = new SourceCounts();

        $sources->count(
            $extractor,
            FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()),
            false,
        );

        static::assertSame([], $sources->statistics());
    }

    public function test_open_returns_the_same_read_per_extractor(): void
    {
        $extractor = from_array([['id' => 1]]);
        $other = from_array([['id' => 2]]);
        $sources = new SourceCounts();

        $read = $sources->open($extractor, false);
        $read->counted(5);

        static::assertSame($read, $sources->open($extractor, true));
        static::assertNotSame($read, $sources->open($other, false));
        static::assertSame(5, $sources->statistics()[0]->rows);
        static::assertFalse($sources->statistics()[0]->complete);
    }
}
