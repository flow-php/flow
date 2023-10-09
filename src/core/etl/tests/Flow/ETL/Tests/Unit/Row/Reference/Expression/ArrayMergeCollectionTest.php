<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\array_merge_collection;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayMergeCollectionTest extends TestCase
{
    public function test_attempt_of_merging_collection_where_not_every_element_is_array() : void
    {
        $row = Row::create(
            Entry::array(
                'array_entry',
                [
                    ['foo' => 'bar'],
                    1,
                ]
            ),
        );

        $this->assertNull(array_merge_collection(ref('array_entry'))->eval($row));
    }

    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            Entry::integer('invalid_entry', 1),
        );

        $this->assertNull(array_merge_collection(ref('invalid_entry'))->eval($row));
    }

    public function test_merging_collection_of_arrays() : void
    {
        $row = Row::create(
            Entry::array(
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

        $this->assertEquals(
            [1, 2],
            array_merge_collection(ref('array_entry'))->eval($row)
        );
    }
}
