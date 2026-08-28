<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_merge_collection;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayMergeCollectionTest extends FlowTestCase
{
    public function test_array_merge_collection_in_strict_mode_with_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        $row = row(int_entry('invalid_entry', 1));

        array_merge_collection(ref('invalid_entry'))->eval($row, $context);
    }

    public function test_array_merge_collection_in_strict_mode_with_non_array_elements(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayMergeCollection function requires array elements to be arrays');

        $context = flow_context(config());
        $row = row(json_entry('array_entry', [
            ['foo' => 'bar'],
            1,
        ]));

        array_merge_collection(ref('array_entry'))->eval($row, $context);
    }

    public function test_attempt_of_merging_collection_where_not_every_element_is_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayMergeCollection function requires array elements to be arrays');

        $row = row(json_entry('array_entry', [
            ['foo' => 'bar'],
            1,
        ]));

        array_merge_collection(ref('array_entry'))->eval($row, flow_context());
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $row = row(int_entry('invalid_entry', 1));

        array_merge_collection(ref('invalid_entry'))->eval($row, flow_context());
    }

    public function test_merging_collection_of_arrays(): void
    {
        $row = row(json_entry('array_entry', [
            [
                1,
            ],
            [
                2,
            ],
            [],
        ]));

        static::assertEquals([1, 2], array_merge_collection(ref('array_entry'))->eval($row, flow_context()));
    }
}
