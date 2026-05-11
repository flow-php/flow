<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class VoidProcessorTest extends FlowTestCase
{
    public function test_discards_all_rows_and_yields_empty_batch(): void
    {
        $processor = new VoidProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(0, $result[0]);
    }

    public function test_handles_empty_input(): void
    {
        $processor = new VoidProcessor();

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(0, $result[0]);
    }
}
