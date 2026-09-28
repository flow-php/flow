<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\CountingProcessor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class CountingProcessorTest extends FlowTestCase
{
    public function test_every_batch_is_counted_into_one_row(): void
    {
        $batches = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield rows(schema(int_schema('id')));
            yield array_to_rows([['id' => 3]], schema(int_schema('id')));
        })();

        static::assertEquals(
            [array_to_rows([['count' => 3]], schema(int_schema('count')))],
            iterator_to_array((new CountingProcessor())->process($batches, flow_context())),
        );
    }

    public function test_no_batch_counts_to_zero(): void
    {
        $batches = (static function () {
            yield from [];
        })();

        static::assertEquals(
            [array_to_rows([['count' => 0]], schema(int_schema('count')))],
            iterator_to_array((new CountingProcessor())->process($batches, flow_context())),
        );
    }

    public function test_bind_outputs_the_count_column_whatever_the_input(): void
    {
        $bound = (new CountingProcessor())->bind(schema(int_schema('id'), str_schema('name')));

        static::assertEquals(schema(int_schema('count')), $bound->output);
        static::assertEquals(new CountingProcessor(), $bound->step);
    }

    public function test_count_builds_with_the_configured_backend(): void
    {
        $backend = new SpyBackend();
        $batches = (static function () {
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
        })();

        iterator_to_array((new CountingProcessor())->process(
            $batches,
            flow_context(config_builder()->backend($backend)->build()),
        ));

        static::assertGreaterThan(0, $backend->builders());
    }
}
