<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class MemorySortProcessorTest extends FlowTestCase
{
    public function test_a_sort_is_ordered_by_its_columns(): void
    {
        $columns = [ref('id')->desc()];

        static::assertSame($columns, (new MemorySortProcessor($columns))->distribution()?->order);
    }

    public function test_bind_returns_the_input_schema_and_declares_it_on_the_rebound_step(): void
    {
        $columns = [ref('id')->asc()];
        $input = schema(int_schema('id'));
        $bound = (new MemorySortProcessor($columns))->bind($input);

        static::assertEquals($input, $bound->output);
        static::assertEquals(new MemorySortProcessor($columns, $input), $bound->step);
    }

    /**
     * A buffer with no rows yields no batch at all, so the declared schema is observable only on a
     * batch that carries rows - there it still wins over the schema the batch arrived with.
     */
    public function test_the_declared_schema_wins_over_the_batch_schema(): void
    {
        $declared = schema(int_schema('id', nullable: true));

        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()], $declared))->process(
            (static function () {
                yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

        static::assertCount(1, $result);
        static::assertEquals($declared, $result[0]->schema());
    }

    public function test_handles_empty_input(): void
    {
        static::assertCount(
            0,
            iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_sorts_across_multiple_batches(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_ascending(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_descending(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->desc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 3], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 3], ['id' => 2], ['id' => 1]], $allRows);
    }
}
