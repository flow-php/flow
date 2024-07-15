<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Flattener;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class FlattenMapsTest extends TestCase
{
    public function test_flattening_empty_map_of_maps() : void
    {
        $column = NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32()));
        $row = [
            'map_of_maps' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_maps.key_value.key' => [],
                'map_of_maps.key_value.value.key_value.key' => [],
                'map_of_maps.key_value.value.key_value.value' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_map_of_string_structure() : void
    {
        $column = NestedColumn::map('map_of_string_struct', MapKey::string(), MapValue::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));
        $row = [
            'map_of_string_struct' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_string_struct.key_value.key' => [],
                'map_of_string_struct.key_value.value.int32' => [],
                'map_of_string_struct.key_value.value.string' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_map_str_int() : void
    {
        $column = NestedColumn::map('map_str_int', MapKey::string(), MapValue::int32());
        $row = [
            'map_str_int' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_str_int.key_value.key' => [],
                'map_str_int.key_value.value' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_map_str_map_list_int() : void
    {
        $column = NestedColumn::map('map_str_list_int', MapKey::string(), MapValue::list(ListElement::int32()));
        $row = [
            'map_str_list_int' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_str_list_int.key_value.key' => [],
                'map_str_list_int.key_value.value.list.element' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_of_empty_maps() : void
    {
        $column = NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32()));
        $row = [
            'map_of_maps' => [
                'a' => [],
                'b' => [],
                'c' => [],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_maps.key_value.key' => ['a', 'b', 'c'],
                'map_of_maps.key_value.value.key_value.key' => [[], [], []],
                'map_of_maps.key_value.value.key_value.value' => [[], [], []],
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
        self::assertSame(
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
        self::assertSame(
            [
                'map_of_maps.key_value.key' => ['a', 'b', 'c'],
                'map_of_maps.key_value.value.key_value.key' => [['d', 'e'], ['f'], null],
                'map_of_maps.key_value.value.key_value.value' => [[1, 2], [null], null],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_map_of_nullable_maps() : void
    {
        $column = NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32()));
        $row = [
            'map_of_maps' => [
                'a' => null,
                'b' => null,
                'c' => null,
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_maps.key_value.key' => ['a', 'b', 'c'],
                'map_of_maps.key_value.value.key_value.key' => [null, null, null],
                'map_of_maps.key_value.value.key_value.value' => [null, null, null],
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
        self::assertSame(
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
        self::assertSame(
            [
                'map_string_int.key_value.key' => ['a', 'b', 'c'],
                'map_string_int.key_value.value' => [0, 1, 2],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_map_of_maps() : void
    {
        $column = NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32()));
        $row = [
            'map_of_maps' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_maps.key_value.key' => null,
                'map_of_maps.key_value.value.key_value.key' => null,
                'map_of_maps.key_value.value.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_map_of_string_structure() : void
    {
        $column = NestedColumn::map('map_of_string_struct', MapKey::string(), MapValue::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));
        $row = [
            'map_of_string_struct' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_of_string_struct.key_value.key' => null,
                'map_of_string_struct.key_value.value.int32' => null,
                'map_of_string_struct.key_value.value.string' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_map_str_int() : void
    {
        $column = NestedColumn::map('map_str_int', MapKey::string(), MapValue::int32());
        $row = [
            'map_str_int' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_str_int.key_value.key' => null,
                'map_str_int.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_map_str_map_list_int() : void
    {
        $column = NestedColumn::map('map_str_list_int', MapKey::string(), MapValue::list(ListElement::int32()));
        $row = [
            'map_str_list_int' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'map_str_list_int.key_value.key' => null,
                'map_str_list_int.key_value.value.list.element' => null,
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
        self::assertSame(
            [
                'map_string_int.key_value.key' => null,
                'map_string_int.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }
}
