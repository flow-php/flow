<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Double\VaryingBatchesExtractor;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\batched_by;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class BatchByExtractorTest extends TestCase
{
    public function test_a_group_spanning_child_batches_answers_to_the_first_batch_schema(): void
    {
        $child = new VaryingBatchesExtractor(
            rows(schema(int_schema('g'), str_schema('name', nullable: true)), row(['g' => 1, 'name' => 'a'])),
            rows(schema(int_schema('g')), row(['g' => 1])),
        );

        $batches = iterator_to_array(batched_by($child, ref('g'))->extract(flow_context(config())), false);

        static::assertCount(1, $batches);
        static::assertSame([['g' => 1, 'name' => 'a'], ['g' => 1, 'name' => null]], $batches[0]->toArray());
    }

    public function test_a_later_child_batch_that_widens_the_shape_is_refused(): void
    {
        $child = new VaryingBatchesExtractor(
            rows(schema(int_schema('g')), row(['g' => 1])),
            rows(schema(int_schema('g'), str_schema('extra')), row(['g' => 1, 'extra' => 'x'])),
        );

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "extra" (row 0) is not declared by the schema');

        iterator_to_array(batched_by($child, ref('g'))->extract(flow_context(config())), false);
    }

    public function test_grouping_by_column_with_min_size(): void
    {
        $extractor = batched_by(
            from_rows(rows(
                schema(int_schema('order_id'), int_schema('item')),
                row(['order_id' => 1, 'item' => 1]),
                row(['order_id' => 2, 'item' => 2]),
                row(['order_id' => 3, 'item' => 3]),
                row(['order_id' => 4, 'item' => 4]),
                row(['order_id' => 5, 'item' => 5]),
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
            from_rows(rows(
                schema(int_schema('order_id'), int_schema('item')),
                row(['order_id' => 1, 'item' => 1]),
                row(['order_id' => 1, 'item' => 2]),
                row(['order_id' => 2, 'item' => 3]),
                row(['order_id' => 2, 'item' => 4]),
                row(['order_id' => 3, 'item' => 5]),
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
            from_rows(rows(
                schema(int_schema('order_id'), int_schema('item')),
                row(['order_id' => 1, 'item' => 1]),
                row(['order_id' => 2, 'item' => 2]),
                row(['order_id' => 3, 'item' => 3]),
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
            from_rows(rows(
                schema(int_schema('order_id'), int_schema('item')),
                row(['order_id' => 1, 'item' => 1]),
                row(['order_id' => 1, 'item' => 2]),
                row(['order_id' => 1, 'item' => 3]),
                row(['order_id' => 1, 'item' => 4]),
                row(['order_id' => 1, 'item' => 5]),
                row(['order_id' => 2, 'item' => 6]),
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
            from_rows(rows(
                schema(int_schema('order_id'), int_schema('item')),
                row(['order_id' => 1, 'item' => 1]),
                row(['order_id' => 1, 'item' => 2]),
                row(['order_id' => 1, 'item' => 3]),
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
        $child = from_rows(rows(schema(int_schema('id')), row(['id' => 1])));

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
}
