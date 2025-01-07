<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_get_collection, array_get_collection_first, int_entry, json_entry, ref};
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayGetCollectionTest extends FlowTestCase
{
    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            int_entry('invalid_entry', 1),
        );

        self::assertNull(array_get_collection(ref('invalid_entry'), ['id'])->eval($row));
    }

    public function test_getting_keys_from_simple_array() : void
    {
        $row = Row::create(
            json_entry(
                'array_entry',
                [
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'array' => ['foo' => 'bar'],
                ]
            ),
        );

        self::assertNull(array_get_collection(ref('array_entry'), ['id'])->eval($row));
    }

    public function test_getting_specific_keys_from_collection_of_array() : void
    {
        $row = Row::create(
            json_entry(
                'array_entry',
                [
                    [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                    [
                        'id' => 2,
                        'status' => 'NEW',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                ]
            ),
        );

        self::assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            array_get_collection(ref('array_entry'), ['id', 'status'])->eval($row)
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array() : void
    {
        $row = Row::create(
            json_entry(
                'array_entry',
                [
                    [
                        'parent_id' => 1,
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                    [
                        'parent_id' => 1,
                        'id' => 2,
                        'status' => 'NEW',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                ]
            ),
        );

        self::assertEquals(
            [
                'parent_id' => 1,
            ],
            ref('array_entry')->arrayGetCollectionFirst('parent_id')->eval($row)
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array_when_first_index_does_not_exists() : void
    {
        $row = Row::create(
            json_entry(
                'array_entry',
                [
                    2 => [
                        'parent_id' => 1,
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                    3 => [
                        'parent_id' => 1,
                        'id' => 2,
                        'status' => 'NEW',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                ]
            ),
        );

        self::assertEquals(
            [
                'parent_id' => 1,
            ],
            array_get_collection_first(ref('array_entry'), 'parent_id')->eval($row)
        );
    }
}
