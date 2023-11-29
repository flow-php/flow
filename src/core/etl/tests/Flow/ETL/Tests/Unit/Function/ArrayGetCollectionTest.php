<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\array_get_collection;
use function Flow\ETL\DSL\array_get_collection_first;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayGetCollectionTest extends TestCase
{
    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            int_entry('invalid_entry', 1),
        );

        $this->assertNull(array_get_collection(ref('invalid_entry'), 'id')->eval($row));
    }

    public function test_getting_keys_from_simple_array() : void
    {
        $row = Row::create(
            array_entry(
                'array_entry',
                [
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'array' => ['foo' => 'bar'],
                ]
            ),
        );

        $this->assertNull(array_get_collection(ref('array_entry'), 'id', 'status')->eval($row));
    }

    public function test_getting_specific_keys_from_collection_of_array() : void
    {
        $row = Row::create(
            array_entry(
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

        $this->assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            array_get_collection(ref('array_entry'), 'id', 'status')->eval($row)
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array() : void
    {
        $row = Row::create(
            array_entry(
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

        $this->assertEquals(
            [
                'parent_id' => 1,
            ],
            array_get_collection_first(ref('array_entry'), 'parent_id')->eval($row)
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array_when_first_index_does_not_exists() : void
    {
        $row = Row::create(
            array_entry(
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

        $this->assertEquals(
            [
                'parent_id' => 1,
            ],
            array_get_collection_first(ref('array_entry'), 'parent_id')->eval($row)
        );
    }
}
