<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\SwappableRowsExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;

final class SwappableRowsExtractorTest extends FlowTestCase
{
    public function test_is_not_stopped_when_extraction_completes_without_a_signal(): void
    {
        $extractor = new SwappableRowsExtractor();
        $extractor->swap(rows(row(int_entry('id', 1))));

        iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertFalse($extractor->stopped());
    }

    public function test_stops_on_stop_signal_sent_on_the_first_yield(): void
    {
        $extractor = new SwappableRowsExtractor();
        $extractor->swap(rows(row(int_entry('id', 1))));

        $generator = $extractor->extract(execution_context(config_builder()->build()));

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertTrue($extractor->stopped());
    }

    public function test_stops_on_stop_signal_sent_after_the_batch_was_consumed(): void
    {
        $extractor = new SwappableRowsExtractor();
        $extractor->swap(rows(row(int_entry('id', 1))));

        $generator = $extractor->extract(execution_context(config_builder()->build()));
        $generator->next();

        static::assertTrue($generator->valid());
        static::assertCount(0, $generator->current());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertTrue($extractor->stopped());
    }

    public function test_yields_currently_held_rows_followed_by_an_empty_batch(): void
    {
        $extractor = new SwappableRowsExtractor();
        $extractor->swap(rows(row(int_entry('id', 1)), row(int_entry('id', 2))));

        $extracted = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertCount(2, $extracted);
        static::assertSame([['id' => 1], ['id' => 2]], $extracted[0]->toArray());
        static::assertCount(0, $extracted[1]);
    }

    public function test_yields_empty_rows_before_first_swap(): void
    {
        $extracted = iterator_to_array((new SwappableRowsExtractor())->extract(
            execution_context(config_builder()->build()),
        ));

        static::assertCount(0, $extracted[0]);
    }

    public function test_yields_swapped_rows_on_next_extraction(): void
    {
        $context = execution_context(config_builder()->build());
        $extractor = new SwappableRowsExtractor();

        $extractor->swap(rows(row(int_entry('id', 1))));
        $first = iterator_to_array($extractor->extract($context));

        $extractor->swap(rows(row(int_entry('id', 2))));
        $second = iterator_to_array($extractor->extract($context));

        static::assertSame([['id' => 1]], $first[0]->toArray());
        static::assertSame([['id' => 2]], $second[0]->toArray());
    }
}
