<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\CountingProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class CountingProcessorTest extends FlowTestCase
{
    public function test_every_batch_is_counted_into_one_row(): void
    {
        $batches = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
            yield rows(schema(int_schema('id')));
            yield rows(schema(int_schema('id')), row(['id' => 3]));
        })();

        static::assertEquals(
            [rows(schema(int_schema('count')), row(['count' => 3]))],
            iterator_to_array((new CountingProcessor())->process($batches, flow_context())),
        );
    }

    public function test_no_batch_counts_to_zero(): void
    {
        $batches = (static function () {
            yield from [];
        })();

        static::assertEquals(
            [rows(schema(int_schema('count')), row(['count' => 0]))],
            iterator_to_array((new CountingProcessor())->process($batches, flow_context())),
        );
    }

    public function test_bind_outputs_the_count_column_whatever_the_input(): void
    {
        $bound = (new CountingProcessor())->bind(schema(int_schema('id'), str_schema('name')));

        static::assertEquals(schema(int_schema('count')), $bound->output);
        static::assertEquals(new CountingProcessor(), $bound->step);
    }
}
