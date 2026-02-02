<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{flow_context, int_entry, row, rows};
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class CollectingProcessorTest extends FlowTestCase
{
    public function test_collects_all_rows_into_single_batch() : void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
            yield rows(row(int_entry('id', 4)), row(int_entry('id', 5)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(5, $result[0]);
        self::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            $result[0]->toArray()
        );
    }

    public function test_handles_empty_input() : void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(0, $result[0]);
    }

    public function test_handles_single_batch() : void
    {
        $processor = new CollectingProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(2, $result[0]);
    }
}
