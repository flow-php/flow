<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_instance_of;

final class DistinctProcessorTest extends FlowTestCase
{
    public function test_a_bound_distinct_is_clustered_on_its_columns_and_runs_per_split_first(): void
    {
        $distinct = type_instance_of(DistinctProcessor::class)->assert((new DistinctProcessor([
            'country',
            ref('day'),
        ]))->bind(schema(str_schema('country'), int_schema('day')))->step);
        $distribution = $distinct->distribution();

        static::assertSame(['country', 'day'], $distribution?->keys);
        static::assertSame($distinct, $distribution->partial);
        static::assertNull($distribution->final);
    }

    public function test_a_distinct_over_a_column_missing_from_its_input_has_no_distribution(): void
    {
        static::assertNull(
            type_instance_of(DistinctProcessor::class)
                ->assert((new DistinctProcessor(['country', 'day']))->bind(schema(str_schema('country')))->step)
                ->distribution(),
        );
    }

    public function test_an_unbound_distinct_has_no_distribution(): void
    {
        static::assertNull((new DistinctProcessor(['country']))->distribution());
    }

    public function test_a_key_seen_in_an_earlier_batch_is_dropped(): void
    {
        static::assertSame(
            [['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2], ['k' => 'c', 'v' => 5]],
            RowsStorageContext::rows((new DistinctProcessor(['k']))->process(
                GroupByContext::batches(
                    array_to_rows(
                        [['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2], ['k' => 'a', 'v' => 3]],
                        schema(str_schema('k'), int_schema('v')),
                    ),
                    array_to_rows(
                        [['k' => 'b', 'v' => 4], ['k' => 'c', 'v' => 5]],
                        schema(str_schema('k'), int_schema('v')),
                    ),
                ),
                flow_context()->backend(),
            )),
        );
    }

    public function test_every_run_starts_with_no_seen_key(): void
    {
        $processor = new DistinctProcessor(['k']);
        $batches = static fn(): Generator => GroupByContext::batches(array_to_rows([
            ['k' => 1],
            ['k' => 1],
        ], schema(int_schema('k'))));

        static::assertSame(
            [['k' => 1]],
            RowsStorageContext::rows($processor->process($batches(), flow_context()->backend())),
        );
        static::assertSame(
            [['k' => 1]],
            RowsStorageContext::rows($processor->process($batches(), flow_context()->backend())),
        );
    }

    public function test_keys_on_every_column_given(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 1, 'name' => 'b'], ['id' => 2, 'name' => 'a']],
            RowsStorageContext::rows((new DistinctProcessor(['id', ref('name')]))->process(
                GroupByContext::batches(array_to_rows(
                    [
                        ['id' => 1, 'name' => 'a'],
                        ['id' => 1, 'name' => 'b'],
                        ['id' => 1, 'name' => 'a'],
                        ['id' => 2, 'name' => 'a'],
                    ],
                    schema(int_schema('id'), str_schema('name')),
                )),
                flow_context()->backend(),
            )),
        );
    }

    public function test_a_null_key_is_kept_once(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => null, 'name' => 'b']],
            RowsStorageContext::rows((new DistinctProcessor(['id']))->process(
                GroupByContext::batches(array_to_rows(
                    [['id' => 1, 'name' => 'a'], ['id' => null, 'name' => 'b'], ['id' => null, 'name' => 'c']],
                    schema(int_schema('id', nullable: true), str_schema('name')),
                )),
                flow_context()->backend(),
            )),
        );
    }

    public function test_zero_and_negative_zero_are_duplicates(): void
    {
        static::assertSame(
            [['v' => 0.0], ['v' => 1.5]],
            RowsStorageContext::rows((new DistinctProcessor(['v']))->process(
                GroupByContext::batches(array_to_rows([
                    ['v' => 0.0],
                    ['v' => -0.0],
                    ['v' => 1.5],
                    ['v' => 1.5],
                ], schema(float_schema('v')))),
                flow_context()->backend(),
            )),
        );
    }

    public function test_no_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DistinctProcessor requires at least one column');

        new DistinctProcessor([]);
    }

    public function test_a_stop_is_forwarded(): void
    {
        $stopped = false;

        $rows = (new DistinctProcessor(['k']))->process(
            (static function () use (&$stopped): Generator {
                // @mago-ignore analysis:mixed-assignment
                $signal = yield array_to_rows([['k' => 1]], schema(int_schema('k')));
                $stopped = $signal === Signal::STOP;

                yield array_to_rows([['k' => 2]], schema(int_schema('k')));
            })(),
            flow_context()->backend(),
        );
        $rows->current();
        $rows->send(Signal::STOP);

        static::assertTrue($stopped);
        static::assertFalse($rows->valid());
    }

    public function test_bind_keeps_the_input_schema(): void
    {
        $bound = (new DistinctProcessor(['k']))->bind(schema(int_schema('k'), str_schema('v')));

        static::assertEquals(schema(int_schema('k'), str_schema('v')), $bound->output);
        static::assertInstanceOf(DistinctProcessor::class, $bound->step);
        static::assertEquals(schema(int_schema('k'), str_schema('v')), $bound->step->input);
        static::assertNull((new DistinctProcessor(['k']))->input);
    }
}
