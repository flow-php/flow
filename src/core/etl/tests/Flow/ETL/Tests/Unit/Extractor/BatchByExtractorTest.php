<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
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
use function iterator_to_array;

final class BatchByExtractorTest extends TestCase
{
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
}
