<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class VoidProcessorTest extends FlowTestCase
{
    public function test_a_zero_batch_input_yields_the_declared_schema(): void
    {
        $declared = schema(int_schema('id'));

        $generator = (static function () {
            yield from [];
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array((new VoidProcessor($declared))->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertEquals($declared, $result[0]->schema());
    }

    public function test_bind_returns_the_input_schema_and_declares_it_on_the_rebound_step(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));
        $bound = (new VoidProcessor())->bind($input);

        static::assertEquals($input, $bound->output);
        static::assertEquals(new VoidProcessor($input), $bound->step);
    }

    public function test_discards_all_rows_and_yields_empty_batch(): void
    {
        $processor = new VoidProcessor();

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id')));
        })();

        /** @var list<Rows> $result */
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

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(0, $result[0]);
    }
}
