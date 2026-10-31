<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\VaryingBatchesExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Rows;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function range;

final class BatchingProcessorTest extends FlowTestCase
{
    /**
     * @param int<1, max> $size
     */
    #[TestWith([2])]
    #[TestWith([5])]
    public function test_a_batch_under_another_schema_is_conformed_to_the_first(int $size): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new BatchingProcessor(new BatchSize($size)))->process(
            (static function () {
                yield array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name', true)));
                yield array_to_rows([['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

        static::assertSame([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $result[0]->toArray());
    }

    public function test_a_later_batch_that_widens_the_shape_is_refused(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "extra" (row 0) is not declared by the schema');

        iterator_to_array(
            (new BatchingProcessor(new BatchSize(2)))->process(
                FlowTestCase::extracted(
                    new VaryingBatchesExtractor(
                        array_to_rows([['id' => 1]], schema(int_schema('id'))),
                        array_to_rows([['id' => 1, 'extra' => 'x']], schema(int_schema('id'), str_schema('extra'))),
                    ),
                    flow_context()->backend(),
                    new Criteria(),
                ),
                flow_context()->backend(),
            ),
            false,
        );
    }

    public function test_batching_processor_forwards_stop_to_its_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new BatchingProcessor(new BatchSize(1)))->process(
            FlowTestCase::extracted($upstream, flow_context()->backend(), new Criteria()),
            flow_context()->backend(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(1, $upstream->batchesYielded);
    }

    public function test_windows_built_from_many_child_batches_keep_counts_and_order(): void
    {
        $children = [];

        for ($batch = 0; $batch < 25; $batch++) {
            $children[] = array_to_rows(
                array_map(static fn(int $id): array => ['id' => $id], range(($batch * 100) + 1, ($batch * 100) + 100)),
                schema(int_schema('id')),
            );
        }

        $windows = iterator_to_array(
            (new BatchingProcessor(new BatchSize(1_000)))->process(
                FlowTestCase::extracted(
                    new VaryingBatchesExtractor(...$children),
                    flow_context()->backend(),
                    new Criteria(),
                ),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame([1000, 1000, 500], array_map(static fn(Rows $rows): int => $rows->count(), $windows));
        static::assertSame(
            range(1, 2_500),
            array_merge(...array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $windows)),
        );
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new BatchingProcessor(new BatchSize(2)))->bind($input)->output);
    }

    public function test_handles_empty_input(): void
    {
        static::assertCount(
            0,
            iterator_to_array((new BatchingProcessor(new BatchSize(2)))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_handles_exact_batch_size_multiple(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new BatchingProcessor(new BatchSize(2)))->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        static::assertCount(2, $result);
        static::assertCount(2, $result[0]);
        static::assertCount(2, $result[1]);
    }

    public function test_handles_single_large_batch(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new BatchingProcessor(new BatchSize(3)))->process(
            (static function () {
                yield array_to_rows([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                ], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        static::assertCount(2, $result);
        static::assertCount(3, $result[0]);
        static::assertCount(2, $result[1]);
    }

    public function test_rebatches_rows_into_fixed_size(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new BatchingProcessor(new BatchSize(2)))->process(
            (static function () {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 2]], schema(int_schema('id')));
                yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                yield array_to_rows([['id' => 4]], schema(int_schema('id')));
                yield array_to_rows([['id' => 5]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        static::assertCount(3, $result);
        static::assertCount(2, $result[0]);
        static::assertCount(2, $result[1]);
        static::assertCount(1, $result[2]);
    }
}
