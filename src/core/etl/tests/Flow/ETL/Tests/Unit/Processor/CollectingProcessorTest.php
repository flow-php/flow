<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class CollectingProcessorTest extends FlowTestCase
{
    public function test_collects_all_rows_into_single_batch(): void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
            yield rows(row(int_entry('id', 4)), row(int_entry('id', 5)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(5, $result[0]);
        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            $result[0]->toArray(),
        );
    }

    public function test_handles_empty_input(): void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield from [];
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(0, $result[0]);
    }

    public function test_handles_single_batch(): void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }
}
