<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollectingProcessorTest extends FlowTestCase
{
    public function test_a_zero_batch_input_yields_the_declared_schema(): void
    {
        $declared = schema(int_schema('id'));

        /** @var list<Rows> $result */
        $result = iterator_to_array((new CollectingProcessor($declared))->process(
            (static function () {
                yield from [];
            })(),
            flow_context()->backend(),
        ));

        static::assertCount(1, $result);
        static::assertEquals($declared, $result[0]->schema());
    }

    public function test_bind_returns_the_input_schema_and_declares_it_on_the_rebound_step(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));
        $bound = (new CollectingProcessor())->bind($input);

        static::assertEquals($input, $bound->output);
        static::assertEquals(new CollectingProcessor($input), $bound->step);
    }

    public function test_collects_all_rows_into_single_batch(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new CollectingProcessor())->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                yield array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

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
        /** @var list<Rows> $result */
        $result = iterator_to_array((new CollectingProcessor())->process(
            (static function () {
                yield from [];
            })(),
            flow_context()->backend(),
        ));

        static::assertCount(1, $result);
        static::assertCount(0, $result[0]);
    }

    public function test_handles_single_batch(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new CollectingProcessor())->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }
}
