<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{batched_by, config, flow_context, from_rows, int_entry, ref, row, rows};
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class BatchByExtractorTest extends TestCase
{
    public function test_grouping_by_column_with_min_size() : void
    {
        $extractor = batched_by(
            from_rows(rows(
                row(int_entry('order_id', 1), int_entry('item', 1)),
                row(int_entry('order_id', 2), int_entry('item', 2)),
                row(int_entry('order_id', 3), int_entry('item', 3)),
                row(int_entry('order_id', 4), int_entry('item', 4)),
                row(int_entry('order_id', 5), int_entry('item', 5))
            )),
            ref('order_id'),
            3
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(2, $batches);
        self::assertCount(3, $batches[0]);
        self::assertCount(2, $batches[1]);
    }

    public function test_grouping_by_column_without_min_size() : void
    {
        $extractor = batched_by(
            from_rows(rows(
                row(int_entry('order_id', 1), int_entry('item', 1)),
                row(int_entry('order_id', 1), int_entry('item', 2)),
                row(int_entry('order_id', 2), int_entry('item', 3)),
                row(int_entry('order_id', 2), int_entry('item', 4)),
                row(int_entry('order_id', 3), int_entry('item', 5))
            )),
            ref('order_id'),
            null
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(3, $batches);
        self::assertCount(2, $batches[0]);
        self::assertCount(2, $batches[1]);
        self::assertCount(1, $batches[2]);
    }

    public function test_grouping_with_all_unique_values() : void
    {
        $extractor = batched_by(
            from_rows(rows(
                row(int_entry('order_id', 1), int_entry('item', 1)),
                row(int_entry('order_id', 2), int_entry('item', 2)),
                row(int_entry('order_id', 3), int_entry('item', 3))
            )),
            ref('order_id'),
            null
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(3, $batches);
        self::assertCount(1, $batches[0]);
        self::assertCount(1, $batches[1]);
        self::assertCount(1, $batches[2]);
    }

    public function test_grouping_with_empty_data() : void
    {
        $extractor = batched_by(
            from_rows(rows()),
            ref('order_id'),
            null
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(0, $batches);
    }

    public function test_grouping_with_large_group_exceeding_min_size() : void
    {
        $extractor = batched_by(
            from_rows(rows(
                row(int_entry('order_id', 1), int_entry('item', 1)),
                row(int_entry('order_id', 1), int_entry('item', 2)),
                row(int_entry('order_id', 1), int_entry('item', 3)),
                row(int_entry('order_id', 1), int_entry('item', 4)),
                row(int_entry('order_id', 1), int_entry('item', 5)),
                row(int_entry('order_id', 2), int_entry('item', 6))
            )),
            ref('order_id'),
            2
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(2, $batches);
        self::assertCount(5, $batches[0]);
        self::assertCount(1, $batches[1]);
    }

    public function test_grouping_with_single_group() : void
    {
        $extractor = batched_by(
            from_rows(rows(
                row(int_entry('order_id', 1), int_entry('item', 1)),
                row(int_entry('order_id', 1), int_entry('item', 2)),
                row(int_entry('order_id', 1), int_entry('item', 3))
            )),
            ref('order_id'),
            null
        );

        $batches = \iterator_to_array($extractor->extract(flow_context(config())));

        self::assertCount(1, $batches);
        self::assertCount(3, $batches[0]);
    }

    public function test_min_size_validation() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0');

        /** @phpstan-ignore-next-line */
        batched_by(from_rows(rows()), ref('order_id'), 0);
    }
}
