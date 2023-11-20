<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use PHPUnit\Framework\TestCase;

final class FlattenerTest extends TestCase
{
    public function test_flattening_flat_column() : void
    {
        $column = FlatColumn::int32('int32');
        $row = [
            'int32' => 1,
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'int32' => 1,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_flat_structure() : void
    {
        $column = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);
        $row = [
            'struct' => [
                'int32' => 1,
                'string' => 'string',
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'struct.int32' => 1,
                'struct.string' => 'string',
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_ints() : void
    {
        $column = NestedColumn::list('list', ListElement::int32());
        $row = [
            'list' => [1, 2, 3],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'list.list.element' => [1, 2, 3],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_lists() : void
    {
        $column = NestedColumn::list('list', ListElement::list(ListElement::int32()));
        $row = [
            'list' => [
                [1, 2, 3],
                [],
                [4, 5, 6],
                null,
                [null, null, null],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'list.list.element.list.element' => [
                    [1, 2, 3],
                    [],
                    [4, 5, 6],
                    null,
                    [null, null, null],
                ],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_maps() : void
    {
        $column = NestedColumn::list('list_of_maps', ListElement::map(MapKey::string(), MapValue::int32()));
        $row = [
            'list_of_maps' => [
                [
                    'a' => 1,
                    'b' => 2,
                ],
                [
                    'c' => 3,
                    'd' => 4,
                ],
                [
                    'e' => 5,
                    'f' => 6,
                ],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'list_of_maps.list.element.key_value.key' => [['a', 'b'], ['c', 'd'], ['e', 'f']],
                'list_of_maps.list.element.key_value.value' => [[1, 2], [3, 4], [5, 6]],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_structs() : void
    {
        $column = NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));
        $row = [
            'list_of_structs' => [
                [
                    'int32' => 1,
                    'string' => 'string',
                ],
                [
                    'int32' => 2,
                    'string' => 'string',
                ],
                [
                    'int32' => 3,
                    'string' => 'string',
                ],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'list_of_structs.list.element.int32' => [1, 2, 3],
                'list_of_structs.list.element.string' => ['string', 'string', 'string'],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_of_lists() : void
    {
        $column = NestedColumn::map('map_of_lists', MapKey::string(), MapValue::list(ListElement::int32()));
        $row = [
            'map_of_lists' => [
                'a' => [1, 2, 3, 4],
                'b' => [null, null],
                'c' => [10, 11],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'map_of_lists.key_value.key' => ['a', 'b', 'c'],
                'map_of_lists.key_value.value.list.element' => [[1, 2, 3, 4], [null, null], [10, 11]],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_of_maps() : void
    {
        $column = NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32()));
        $row = [
            'map_of_maps' => [
                'a' => [
                    'd' => 1,
                    'e' => 2,
                ],
                'b' => [
                    'f' => null,
                ],
                'c' => null,
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'map_of_maps.key_value.key' => ['a', 'b', 'c'],
                'map_of_maps.key_value.value.key_value.key' => [['d', 'e'], ['f'], null],
                'map_of_maps.key_value.value.key_value.value' => [[1, 2], [null], null],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_of_string_structure() : void
    {
        $column = NestedColumn::map('map_of_string_struct', MapKey::string(), MapValue::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));
        $row = [
            'map_of_string_struct' => [
                'a' => [
                    'int32' => 1,
                    'string' => 'string',
                ],
                'b' => [
                    'int32' => 2,
                    'string' => 'string',
                ],
                'c' => [
                    'int32' => 3,
                    'string' => 'string',
                ],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'map_of_string_struct.key_value.key' => ['a', 'b', 'c'],
                'map_of_string_struct.key_value.value.int32' => [1, 2, 3],
                'map_of_string_struct.key_value.value.string' => ['string', 'string', 'string'],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_string_int() : void
    {
        $column = NestedColumn::map('map_string_int', MapKey::string(), MapValue::int32());
        $row = [
            'map_string_int' => [
                'a' => 0,
                'b' => 1,
                'c' => 2,
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'map_string_int.key_value.key' => ['a', 'b', 'c'],
                'map_string_int.key_value.value' => [0, 1, 2],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_list_of_ints() : void
    {
        $column = NestedColumn::list('list', ListElement::int32());
        $row = [
            'list' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'list.list.element' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_map_string_int() : void
    {
        $column = NestedColumn::map('map_string_int', MapKey::string(), MapValue::int32());
        $row = [
            'map_string_int' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'map_string_int.key_value.key' => null,
                'map_string_int.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_structure_with_list_of_ints_and_map_string_string() : void
    {
        $column = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_ints', ListElement::int32()),
            NestedColumn::map('map_string_string', MapKey::string(), MapValue::string()),
        ]);
        $row = [
            'struct' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'struct.int32' => null,
                'struct.list_of_ints.list.element' => null,
                'struct.map_string_string.key_value.key' => null,
                'struct.map_string_string.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_structure_with_list_of_ints_and_map_string_string() : void
    {
        $column = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_ints', ListElement::int32()),
            NestedColumn::map('map_string_string', MapKey::string(), MapValue::string()),
        ]);
        $row = [
            'struct' => [
                'int32' => 1,
                'list_of_ints' => [1, 2, 3],
                'map_string_string' => [
                    'a' => 'a',
                    'b' => 'b',
                    'c' => 'c',
                ],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame(
            [
                'struct.int32' => 1,
                'struct.list_of_ints.list.element' => [1, 2, 3],
                'struct.map_string_string.key_value.key' => ['a', 'b', 'c'],
                'struct.map_string_string.key_value.value' => ['a', 'b', 'c'],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_when_column_is_not_present_in_row() : void
    {
        $column = FlatColumn::int32('int32');
        $row = [];

        $flattener = new Flattener(new DisabledValidator());
        $this->assertSame([], $flattener->flattenColumn($column, $row));
    }
}
