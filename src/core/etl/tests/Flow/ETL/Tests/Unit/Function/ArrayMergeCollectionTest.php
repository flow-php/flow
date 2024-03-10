<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, array_merge_collection, int_entry, ref};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayMergeCollectionTest extends TestCase
{
    public function test_attempt_of_merging_collection_where_not_every_element_is_array() : void
    {
        $row = Row::create(
            array_entry(
                'array_entry',
                [
                    ['foo' => 'bar'],
                    1,
                ]
            ),
        );

        self::assertNull(array_merge_collection(ref('array_entry'))->eval($row));
    }

    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            int_entry('invalid_entry', 1),
        );

        self::assertNull(array_merge_collection(ref('invalid_entry'))->eval($row));
    }

    public function test_merging_collection_of_arrays() : void
    {
        $row = Row::create(
            array_entry(
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
            ),
        );

        self::assertEquals(
            [1, 2],
            array_merge_collection(ref('array_entry'))->eval($row)
        );
    }
}
