<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Flattener;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class FlattenListsTest extends TestCase
{
    public function test_flattening_empty_list_of_ints() : void
    {
        $column = NestedColumn::list('list', ListElement::int32());
        $row = [
            'list' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list.list.element' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_list_of_lists() : void
    {
        $column = NestedColumn::list('list', ListElement::list(ListElement::int32()));
        $row = [
            'list' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list.list.element.list.element' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_list_of_maps() : void
    {
        $column = NestedColumn::list('list_of_maps', ListElement::map(MapKey::string(), MapValue::int32()));
        $row = [
            'list_of_maps' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_maps.list.element.key_value.key' => [],
                'list_of_maps.list.element.key_value.value' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_empty_list_of_structs() : void
    {
        $column = NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));

        $row = [
            'list_of_structs' => [],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_structs.list.element.int32' => [],
                'list_of_structs.list.element.string' => [],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_empty_lists() : void
    {
        $column = NestedColumn::list('list', ListElement::list(ListElement::int32()));
        $row = [
            'list' => [
                [],
                [],
                [],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list.list.element.list.element' => [[], [], []],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_empty_maps() : void
    {
        $column = NestedColumn::list('list_of_maps', ListElement::map(MapKey::string(), MapValue::int32()));
        $row = [
            'list_of_maps' => [
                [],
                [],
                [],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_maps.list.element.key_value.key' => [[], [], []],
                'list_of_maps.list.element.key_value.value' => [[], [], []],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_half_nullable_structs() : void
    {
        $column = NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));

        $row = [
            'list_of_structs' => [
                [
                    'int32' => null,
                    'string' => 'test',
                ],
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_structs.list.element.int32' => [0 => null],
                'list_of_structs.list.element.string' => [0 => 'test'],
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
        self::assertSame(
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
        self::assertSame(
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
        self::assertSame(
            [
                'list_of_maps.list.element.key_value.key' => [['a', 'b'], ['c', 'd'], ['e', 'f']],
                'list_of_maps.list.element.key_value.value' => [[1, 2], [3, 4], [5, 6]],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_nullable_lists() : void
    {
        $column = NestedColumn::list('list', ListElement::list(ListElement::int32()));
        $row = [
            'list' => [
                null,
                null,
                null,
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list.list.element.list.element' => [null, null, null],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_list_of_nullable_maps() : void
    {
        $column = NestedColumn::list('list_of_maps', ListElement::map(MapKey::string(), MapValue::int32()));
        $row = [
            'list_of_maps' => [
                null,
                null,
                null,
            ],
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_maps.list.element.key_value.key' => [null, null, null],
                'list_of_maps.list.element.key_value.value' => [null, null, null],
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
        self::assertSame(
            [
                'list_of_structs.list.element.int32' => [1, 2, 3],
                'list_of_structs.list.element.string' => ['string', 'string', 'string'],
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
        self::assertSame(
            [
                'list.list.element' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_list_of_lists() : void
    {
        $column = NestedColumn::list('list', ListElement::list(ListElement::int32()));
        $row = [
            'list' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list.list.element.list.element' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_list_of_maps() : void
    {
        $column = NestedColumn::list('list_of_maps', ListElement::map(MapKey::string(), MapValue::int32()));
        $row = [
            'list_of_maps' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_maps.list.element.key_value.key' => null,
                'list_of_maps.list.element.key_value.value' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_nullable_list_of_structs() : void
    {
        $column = NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));
        $row = [
            'list_of_structs' => null,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'list_of_structs.list.element.int32' => null,
                'list_of_structs.list.element.string' => null,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }
}
