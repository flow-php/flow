<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{array_merge_collection, int_entry, json_entry, ref};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayMergeCollectionTest extends FlowTestCase
{
    public function test_attempt_of_merging_collection_where_not_every_element_is_array() : void
    {
        $row = row(json_entry(
            'array_entry',
            [
                ['foo' => 'bar'],
                1,
            ]
        ));

        self::assertNull(array_merge_collection(ref('array_entry'))->eval($row));
    }

    public function test_for_not_array_entry() : void
    {
        $row = row(int_entry('invalid_entry', 1));

        self::assertNull(array_merge_collection(ref('invalid_entry'))->eval($row));
    }

    public function test_merging_collection_of_arrays() : void
    {
        $row = row(json_entry(
            'array_entry',
            [
                [
                    1,
                ],
                [
                    2,
                ],
                [],
            ]
        ));

        self::assertEquals(
            [1, 2],
            array_merge_collection(ref('array_entry'))->eval($row)
        );
    }
}
