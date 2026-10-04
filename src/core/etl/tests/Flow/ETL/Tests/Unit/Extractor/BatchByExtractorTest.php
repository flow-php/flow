<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use DateTimeImmutable;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\DeclaringExtractor;
use Flow\ETL\Tests\Double\VaryingBatchesExtractor;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\batched_by;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function iterator_to_array;

final class BatchByExtractorTest extends TestCase
{
    public function test_a_group_spanning_child_batches_answers_to_the_first_batch_schema(): void
    {
        $child = new VaryingBatchesExtractor(
            array_to_rows([['g' => 1, 'name' => 'a']], schema(int_schema('g'), str_schema('name', nullable: true))),
            array_to_rows([['g' => 1]], schema(int_schema('g'))),
        );

        $batches = iterator_to_array(batched_by($child, ref('g'))->extract(flow_context(config())), false);

        static::assertCount(1, $batches);
        static::assertSame([['g' => 1, 'name' => 'a'], ['g' => 1, 'name' => null]], $batches[0]->toArray());
    }

    public function test_equal_uuids_stay_in_one_batch(): void
    {
        $batches = iterator_to_array(
            batched_by(
                from_rows(array_to_rows(
                    [
                        ['id' => 1, 'u' => new Uuid('00000000-0000-4000-8000-000000000001')],
                        ['id' => 2, 'u' => new Uuid('00000000-0000-4000-8000-000000000001')],
                        ['id' => 3, 'u' => new Uuid('00000000-0000-4000-8000-000000000002')],
                    ],
                    schema(int_schema('id'), uuid_schema('u')),
                )),
                ref('u'),
            )->extract(flow_context(config())),
            false,
        );

        static::assertSame(
            [[1, 2], [3]],
            array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $batches),
        );
    }

    public function test_equal_datetimes_stay_in_one_batch(): void
    {
        $batches = iterator_to_array(
            batched_by(
                from_rows(array_to_rows(
                    [
                        ['id' => 1, 'at' => new DateTimeImmutable('2026-01-01 00:00:00')],
                        ['id' => 2, 'at' => new DateTimeImmutable('2026-01-01 00:00:00')],
                        ['id' => 3, 'at' => new DateTimeImmutable('2026-01-02 00:00:00')],
                    ],
                    schema(int_schema('id'), datetime_schema('at')),
                )),
                ref('at'),
            )->extract(flow_context(config())),
            false,
        );

        static::assertSame(
            [[1, 2], [3]],
            array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $batches),
        );
    }

    public function test_nulls_are_a_group_of_their_own(): void
    {
        $batches = iterator_to_array(
            batched_by(
                from_rows(array_to_rows(
                    [['id' => 1, 'g' => null], ['id' => 2, 'g' => null], ['id' => 3, 'g' => 1]],
                    schema(int_schema('id'), int_schema('g', nullable: true)),
                )),
                ref('g'),
            )->extract(flow_context(config())),
            false,
        );

        static::assertSame(
            [[1, 2], [3]],
            array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $batches),
        );
    }

    public function test_a_group_spanning_two_child_batches_is_one_batch(): void
    {
        $child = new VaryingBatchesExtractor(
            array_to_rows([['g' => 1], ['g' => 1]], schema(int_schema('g'))),
            array_to_rows([['g' => 1], ['g' => 2]], schema(int_schema('g'))),
        );

        $batches = iterator_to_array(batched_by($child, ref('g'))->extract(flow_context(config())), false);

        static::assertSame(
            [[1, 1, 1], [2]],
            array_map(static fn(Rows $rows): array => $rows->reduceToArray('g'), $batches),
        );
    }

    public function test_a_group_spanning_two_child_batches_counts_toward_the_min_size(): void
    {
        $child = new VaryingBatchesExtractor(
            array_to_rows([['g' => 1], ['g' => 1]], schema(int_schema('g'))),
            array_to_rows([['g' => 1], ['g' => 2]], schema(int_schema('g'))),
        );

        $batches = iterator_to_array(batched_by($child, ref('g'), 3)->extract(flow_context(config())), false);

        static::assertSame(
            [[1, 1, 1], [2]],
            array_map(static fn(Rows $rows): array => $rows->reduceToArray('g'), $batches),
        );
    }

    public function test_a_later_child_batch_that_widens_the_shape_is_refused(): void
    {
        $child = new VaryingBatchesExtractor(
            array_to_rows([['g' => 1]], schema(int_schema('g'))),
            array_to_rows([['g' => 1, 'extra' => 'x']], schema(int_schema('g'), str_schema('extra'))),
        );

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "extra" (row 0) is not declared by the schema');

        iterator_to_array(batched_by($child, ref('g'))->extract(flow_context(config())), false);
    }

    public function test_grouping_by_column_with_min_size(): void
    {
        $extractor = batched_by(
            from_rows(array_to_rows(
                [
                    ['order_id' => 1, 'item' => 1],
                    ['order_id' => 2, 'item' => 2],
                    ['order_id' => 3, 'item' => 3],
                    ['order_id' => 4, 'item' => 4],
                    ['order_id' => 5, 'item' => 5],
                ],
                schema(int_schema('order_id'), int_schema('item')),
            )),
            ref('order_id'),
            3,
        );
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(2, $batches);
        static::assertCount(3, $batches[0]);
        static::assertCount(2, $batches[1]);
    }

    public function test_grouping_by_column_without_min_size(): void
    {
        $extractor = batched_by(
            from_rows(array_to_rows(
                [
                    ['order_id' => 1, 'item' => 1],
                    ['order_id' => 1, 'item' => 2],
                    ['order_id' => 2, 'item' => 3],
                    ['order_id' => 2, 'item' => 4],
                    ['order_id' => 3, 'item' => 5],
                ],
                schema(int_schema('order_id'), int_schema('item')),
            )),
            ref('order_id'),
            null,
        );
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(3, $batches);
        static::assertCount(2, $batches[0]);
        static::assertCount(2, $batches[1]);
        static::assertCount(1, $batches[2]);
    }

    public function test_grouping_with_all_unique_values(): void
    {
        $extractor = batched_by(
            from_rows(array_to_rows(
                [['order_id' => 1, 'item' => 1], ['order_id' => 2, 'item' => 2], ['order_id' => 3, 'item' => 3]],
                schema(int_schema('order_id'), int_schema('item')),
            )),
            ref('order_id'),
            null,
        );
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(3, $batches);
        static::assertCount(1, $batches[0]);
        static::assertCount(1, $batches[1]);
        static::assertCount(1, $batches[2]);
    }

    public function test_grouping_with_empty_data(): void
    {
        $extractor = batched_by(from_rows(rows(schema())), ref('order_id'), null);
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(0, $batches);
    }

    public function test_grouping_with_large_group_exceeding_min_size(): void
    {
        $extractor = batched_by(
            from_rows(array_to_rows(
                [
                    ['order_id' => 1, 'item' => 1],
                    ['order_id' => 1, 'item' => 2],
                    ['order_id' => 1, 'item' => 3],
                    ['order_id' => 1, 'item' => 4],
                    ['order_id' => 1, 'item' => 5],
                    ['order_id' => 2, 'item' => 6],
                ],
                schema(int_schema('order_id'), int_schema('item')),
            )),
            ref('order_id'),
            2,
        );
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(2, $batches);
        static::assertCount(5, $batches[0]);
        static::assertCount(1, $batches[1]);
    }

    public function test_grouping_with_single_group(): void
    {
        $extractor = batched_by(
            from_rows(array_to_rows(
                [['order_id' => 1, 'item' => 1], ['order_id' => 1, 'item' => 2], ['order_id' => 1, 'item' => 3]],
                schema(int_schema('order_id'), int_schema('item')),
            )),
            ref('order_id'),
            null,
        );
        $batches = iterator_to_array($extractor->extract(flow_context(config())));
        static::assertCount(1, $batches);
        static::assertCount(3, $batches[0]);
    }

    public function test_min_size_validation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0');
        // @mago-ignore analysis:invalid-argument
        batched_by(from_rows(rows(schema())), ref('order_id'), 0);
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        iterator_to_array(
            batched_by($child, ref('id'))
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
            false,
        );

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame(
            [['id' => 1]],
            iterator_to_array(batched_by($child, ref('id'))->extract(flow_context()), false)[0]->toArray(),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            batched_by(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))), ref('id'))->isRepeatable(),
        );
    }

    public function test_it_passes_through_the_child_statistics(): void
    {
        $child = new DeclaringExtractor(new Statistics(Cardinality::exact(3), Cardinality::exact(300)));
        $extractor = batched_by($child, 'id');

        static::assertEquals(new Statistics(Cardinality::exact(3), Cardinality::exact(300)), $extractor->statistics());

        $child->statistics = new Statistics(Cardinality::exact(1));

        static::assertEquals(new Statistics(Cardinality::exact(1)), $extractor->statistics());
        static::assertEquals(new Statistics(), batched_by(from_memory(new ArrayMemory()), 'id')->statistics());
    }
}
