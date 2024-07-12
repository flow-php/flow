<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Flattener;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class FlattenStructuresTest extends TestCase
{
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
        self::assertSame(
            [
                'struct.int32' => 1,
                'struct.string' => 'string',
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
        self::assertSame(
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
        self::assertSame(
            [
                'struct.int32' => 1,
                'struct.list_of_ints.list.element' => [1, 2, 3],
                'struct.map_string_string.key_value.key' => ['a', 'b', 'c'],
                'struct.map_string_string.key_value.value' => ['a', 'b', 'c'],
            ],
            $flattener->flattenColumn($column, $row)
        );
    }
}
