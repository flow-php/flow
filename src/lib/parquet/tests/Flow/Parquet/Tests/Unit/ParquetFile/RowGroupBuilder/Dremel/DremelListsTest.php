<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Dremel;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\ColumnDataValidator;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{DremelAssembler, DremelShredder, FlatColumnData};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{ListElement, MapKey, MapValue, NestedColumn, Repetition};
use PHPUnit\Framework\Attributes\{TestWith};
use PHPUnit\Framework\TestCase;

final class DremelListsTest extends TestCase
{
    #[TestWith([
        [
            ['l' => null],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => []],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [null]],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [1, 2, 3, null, 4]],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1, 1, 1, 1],
                'definition_levels' => [3, 3, 3, 2, 3],
                'values' => [1, 2, 3, 4],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [1, 2]],
            ['l' => [3]],
            ['l' => [null]],
            ['l' => [4, 5]],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1, 0, 0, 0, 1],
                'definition_levels' => [3, 3, 3, 2, 3, 3],
                'values' => [1, 2, 3, 4, 5],
            ],
        ],
    ])]
    public function test_optional_list_optional_int32(array $rows, array $expectedColumnData) : void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32()));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element')->repetitions());
        self::assertEquals(3, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedColumnData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            ['l' => null],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => []],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [null]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [[]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [[null]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [[1]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [5],
                'values' => [1],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [[1, 2]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [5, 5],
                'values' => [1, 2],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [[1, null]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [5, 4],
                'values' => [1],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    [1, 2, 3, 4],
                    [5, 6, 7, 8],
                ],
            ],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2, 2, 2, 1, 2, 2, 2],
                'definition_levels' => [5, 5, 5, 5, 5, 5, 5, 5],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_int32(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::int32()
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.list.element')->repetitions());
        self::assertEquals(5, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());
        self::assertEquals(
            $rows,
            (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData)
        );
    }

    #[TestWith([
        [
            [
                'l' => [ // level 0
                    [ // level 1
                        [ // level 2
                            'a' => 1, // level 3  // r: 0
                            'b' => 2,              // r: 3
                        ],
                        [
                            'c' => 3,              // r: 2
                        ],
                    ],
                    [
                        [
                            'd' => 4,             // r: 1
                            'e' => 5,              // r: 3
                        ],
                    ],
                    [
                        [
                            'f' => 6,              // r: 1
                        ],
                        [
                            'g' => 7,              // r: 2
                        ],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0, 3, 2, 1, 3, 1, 2],
                'definition_levels' => [6, 6, 6, 6, 6, 6, 6],
                'values' => ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            ],
            'l.list.element.list.element.key_value.value' => [
                'repetition_levels' => [0, 3, 2, 1, 3, 1, 2],
                'definition_levels' => [7, 7, 7, 7, 7, 7, 7],
                'values' => [1, 2, 3, 4, 5, 6, 7],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_map_string_optional_int32(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::map(
                        MapKey::string(),
                        MapValue::int32()
                    ),
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.list.element.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.list.element.key_value.value')->repetitions());

        self::assertEquals(6, $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(3, $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(7, $schema->get('l.list.element.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(3, $schema->get('l.list.element.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());

        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            [
                'l' => [
                    [
                        ['a' => ['int32' => 1, 'string' => 'A', 'list' => [1, 2, 3], 'map' => ['AA' => 'value01']]],
                    ],
                    [
                        ['b' => ['int32' => 2, 'string' => 'B', 'list' => [4, 5, 6], 'map' => ['BB' => 'value02']]],
                    ],
                    [
                        ['c' => ['int32' => 3, 'string' => 'C', 'list' => [7, 8, 9], 'map' => ['CC' => 'value03']]],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [6, 6, 6],
                'values' => ['a', 'b', 'c'],
            ],
            'l.list.element.list.element.key_value.value.int32' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [8, 8, 8],
                'values' => [1, 2, 3],
            ],
            'l.list.element.list.element.key_value.value.string' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [8, 8, 8],
                'values' => ['A', 'B', 'C'],
            ],
            'l.list.element.list.element.key_value.value.list.list.element' => [
                'repetition_levels' => [0, 4, 4, 1, 4, 4, 1, 4, 4],
                'definition_levels' => [10, 10, 10, 10, 10, 10, 10, 10, 10],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8, 9],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.key' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [9, 9, 9],
                'values' => ['AA', 'BB', 'CC'],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.value' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [10, 10, 10],
                'values' => ['value01', 'value02', 'value03'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    [
                        ['a' => ['int32' => 1, 'string' => 'A', 'list' => [1, 2, 3], 'map' => ['AA' => 'value01']]],
                        ['b' => ['int32' => 2, 'string' => 'B', 'list' => [4, 5, 6], 'map' => ['BB' => 'value02']]],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [6, 6],
                'values' => ['a', 'b'],
            ],
            'l.list.element.list.element.key_value.value.int32' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [8, 8],
                'values' => [1, 2],
            ],
            'l.list.element.list.element.key_value.value.string' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [8, 8],
                'values' => ['A', 'B'],
            ],
            'l.list.element.list.element.key_value.value.list.list.element' => [
                'repetition_levels' => [0, 4, 4, 2, 4, 4],
                'definition_levels' => [10, 10, 10, 10, 10, 10],
                'values' => [1, 2, 3, 4, 5, 6],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.key' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [9, 9],
                'values' => ['AA', 'BB'],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.value' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [10, 10],
                'values' => ['value01', 'value02'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    [
                        null,
                        ['a' => null],
                        ['b' => ['int32' => 1, 'string' => 'A', 'list' => [1, 2, 3], 'map' => ['AA' => 'value01']]],
                        ['c' => ['int32' => 2, 'string' => 'B', 'list' => [4, 5, 6], 'map' => ['BB' => 'value02']]],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0, 2, 2, 2],
                'definition_levels' => [4, 6, 6, 6],
                'values' => ['a', 'b',  'c'],
            ],
            'l.list.element.list.element.key_value.value.int32' => [
                'repetition_levels' => [0, 2, 2, 2],
                'definition_levels' => [4, 6, 8, 8],
                'values' => [1, 2],
            ],
            'l.list.element.list.element.key_value.value.string' => [
                'repetition_levels' => [0, 2, 2, 2],
                'definition_levels' => [4, 6, 8, 8],
                'values' => ['A', 'B'],
            ],
            'l.list.element.list.element.key_value.value.list.list.element' => [
                'repetition_levels' => [0, 2, 2, 4, 4, 2, 4, 4],
                'definition_levels' => [4, 6, 10, 10, 10, 10, 10, 10],
                'values' => [1, 2, 3, 4, 5, 6],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.key' => [
                'repetition_levels' => [0, 2, 2, 2],
                'definition_levels' => [4, 6, 9, 9],
                'values' => ['AA', 'BB'],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.value' => [
                'repetition_levels' => [0, 2, 2, 2],
                'definition_levels' => [4, 6, 10, 10],
                'values' => ['value01', 'value02'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    [
                        ['a' => null],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => ['a'],
            ],
            'l.list.element.list.element.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => [],
            ],
            'l.list.element.list.element.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => [],
            ],
            'l.list.element.list.element.key_value.value.list.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => [],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => [],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [6],
                'values' => [],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_map_string_optional_struct_optional_int32_optional_list_optional_int32_optional_map_string_optional_string(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::map(
                        MapKey::string(),
                        MapValue::structure(
                            [
                                Schema\FlatColumn::int32('int32'),
                                Schema\FlatColumn::string('string'),
                                NestedColumn::list(
                                    'list',
                                    ListElement::int32(),
                                ),
                                NestedColumn::map(
                                    'map',
                                    MapKey::string(),
                                    MapValue::string()
                                ),
                            ],
                        ),
                    ),
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.list.element.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.list.element.key_value.value.string')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.list.element.key_value.value.list.list.element')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.list.element.key_value.value.map.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.list.element.key_value.value.map.key_value.value')->repetitions());

        self::assertEquals(6, $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(3, $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(8, $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(3, $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(8, $schema->get('l.list.element.list.element.key_value.value.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(3, $schema->get('l.list.element.list.element.key_value.value.string')->repetitions()->maxRepetitionLevel());

        self::assertEquals(10, $schema->get('l.list.element.list.element.key_value.value.list.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(4, $schema->get('l.list.element.list.element.key_value.value.list.list.element')->repetitions()->maxRepetitionLevel());

        self::assertEquals(9, $schema->get('l.list.element.list.element.key_value.value.map.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(4, $schema->get('l.list.element.list.element.key_value.value.map.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(10, $schema->get('l.list.element.list.element.key_value.value.map.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(4, $schema->get('l.list.element.list.element.key_value.value.map.key_value.value')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());
        self::assertEquals(
            $rows,
            $assembledRows = (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData),
            'Expected rows: ' . json_encode($rows, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT) . "\n" . 'Actual rows: ' . json_encode($assembledRows, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)
        );
    }

    #[TestWith([
        [
            [
                'l' => [
                    [
                        ['int32' => 1, 'string' => 'A'],
                    ],
                    [
                        ['int32' => 2, 'string' => 'B'],
                        ['int32' => 3, 'string' => 'C'],
                    ],
                ],
            ],
            [
                'l' => [
                    [
                        ['int32' => 1, 'string' => 'A'],
                    ],
                    [
                        ['int32' => 2, 'string' => 'B'],
                        ['int32' => 3, 'string' => 'C'],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.int32' => [
                'repetition_levels' => [0, 1, 2, 0, 1, 2],
                'definition_levels' => [6, 6, 6, 6, 6, 6],
                'values' => [1, 2, 3, 1, 2, 3],
            ],
            'l.list.element.list.element.string' => [
                'repetition_levels' => [0, 1, 2, 0, 1, 2],
                'definition_levels' => [6, 6, 6, 6, 6, 6],
                'values' => ['A', 'B', 'C', 'A', 'B', 'C'],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_struct_optional_int32_optional_string(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::structure(
                        [
                            Schema\FlatColumn::int32('int32'),
                            Schema\FlatColumn::string('string'),
                        ],
                    ),
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.list.element.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.list.element.string')->repetitions());

        self::assertEquals(6, $schema->get('l.list.element.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(6, $schema->get('l.list.element.list.element.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element.string')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());

        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith(
        [
            [
                ['l' => null],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => []],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => null],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => []],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [null]],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [[]]],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [3],
                    'values' => [],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [3],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [['a' => null]]],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [4],
                    'values' => ['a'],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [4],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [['a' => 1]]],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [4],
                    'values' => ['a'],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [5],
                    'values' => [1],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [
                    ['a' => 1, 'b' => 2],
                    ['c' => 3, 'd' => 4]],
                ],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0, 2, 1, 2],
                    'definition_levels' => [4, 4, 4, 4],
                    'values' => ['a', 'b', 'c', 'd'],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0, 2, 1, 2],
                    'definition_levels' => [5, 5, 5, 5],
                    'values' => [1, 2, 3, 4],
                ],
            ],
        ]
    )]
    public function test_optional_list_optional_map_string_optional_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::map(
                    MapKey::string(),
                    MapValue::int32()
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.key_value.value')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData, $rows));
        }
    }

    #[TestWith([
        [
            [
                'l' => [
                    ['int32' => 1, 'l' => null],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [1],
            ],
            'l.list.element.l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => null,
            ],
            [
                'l' => [],
            ],
            [
                'l' => [null],
            ],
            [
                'l' => [
                    ['int32' => null, 'l' => null],
                ],
            ],
            [
                'l' => [
                    ['int32' => 1, 'l' => null],
                ],
            ],
            [
                'l' => [
                    ['int32' => 2, 'l' => []],
                ],
            ],
            [
                'l' => [
                    ['int32' => 3, 'l' => [null]],
                ],
            ],
            [
                'l' => [
                    ['int32' => 4, 'l' => ['a']],
                ],
            ],
            [
                'l' => [
                    ['int32' => 5, 'l' => ['b', 'c']],
                    ['int32' => 6, 'l' => ['d', 'e', 'f']],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                'definition_levels' => [0, 1, 2, 3, 4, 4, 4, 4, 4, 4],
                'values' => [1, 2, 3, 4, 5, 6],
            ],
            'l.list.element.l.list.element' => [
                'repetition_levels' => [0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 2],
                'definition_levels' => [0, 1, 2, 3, 3, 4, 5, 6, 6, 6, 6, 6, 6],
                'values' => ['a', 'b', 'c', 'd', 'e', 'f'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    ['int32' => 4, 'l' => ['a']],
                ],
            ],
            [
                'l' => [
                    ['int32' => 5, 'l' => ['b', 'c']],
                    ['int32' => 6, 'l' => ['d', 'e', 'f']],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 0, 1],
                'definition_levels' => [4, 4, 4],
                'values' => [4, 5, 6],
            ],
            'l.list.element.l.list.element' => [
                'repetition_levels' => [0, 0, 2, 1, 2, 2],
                'definition_levels' => [6, 6, 6, 6, 6, 6],
                'values' => ['a', 'b', 'c', 'd', 'e', 'f'],
            ],
        ],
    ])]
    public function test_optional_list_optional_struct_optional_int32_optional_list_string(array $rows, array $expectedColumnData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::structure(
                    [
                        Schema\FlatColumn::int32('int32'),
                        NestedColumn::list('l', ListElement::string()),
                    ],
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.l.list.element')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(6, $schema->get('l.list.element.l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.l.list.element')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        //        self::assertEquals($expectedColumnData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            [
                'l' => [
                    null,
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            'l.list.element.m.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            'l.list.element.m.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    ['int32' => 100, 'm' => [1 => 'a', 2 => 'b']],
                ],
            ],
            [
                'l' => [
                    ['int32' => 101, 'm' => []],
                ],
            ],
            [
                'l' => [
                    ['int32' => 102, 'm' => null],
                ],
            ],
            [
                'l' => [
                    null,
                ],
            ],
            [
                'l' => null,
            ],
            [
                'l' => [],
            ],
            [
                'l' => [
                    ['int32' => 103, 'm' => [3 => null]],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 0, 0, 0, 0, 0, 0],
                'definition_levels' => [4, 4, 4, 2, 0, 1, 4],
                'values' => [100, 101, 102, 103],
            ],
            'l.list.element.m.key_value.key' => [
                'repetition_levels' => [0, 2, 0, 0, 0, 0, 0, 0],
                'definition_levels' => [5, 5, 4, 3, 2, 0, 1, 5],
                'values' => [1, 2, 3],
            ],
            'l.list.element.m.key_value.value' => [
                'repetition_levels' => [0, 2, 0, 0, 0, 0, 0, 0],
                'definition_levels' => [6, 6, 4, 3, 2, 0, 1, 5],
                'values' => ['a', 'b'],
            ],
        ],
    ])]
    public function test_optional_list_optional_struct_optional_int32_optional_map_int32_optional_string(array $rows, array $expectedColumnData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::structure(
                    [
                        Schema\FlatColumn::int32('int32'),
                        NestedColumn::map('m', MapKey::int32(), MapValue::string()),
                    ],
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.m.key_value.value')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('l.list.element.m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(6, $schema->get('l.list.element.m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.m.key_value.value')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedColumnData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            ['l' => null],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => []],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [null]],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [['int32' => null, 'string' => null]]],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['l' => [['int32' => 1, 'string' => 'a']]],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [1],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => ['a'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => null,
            ],
            [
                'l' => [],
            ],
            [
                'l' => [null],
            ],
            [
                'l' => [
                    ['int32' => null, 'string' => null],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 0, 0, 0],
                'definition_levels' => [0, 1, 2, 3],
                'values' => [],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0, 0, 0, 0],
                'definition_levels' => [0, 1, 2, 3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    ['int32' => 1, 'string' => 'a'],
                ],
            ],
            [
                'l' => [
                    ['int32' => 2, 'string' => 'b'],
                    ['int32' => 3, 'string' => 'c'],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 0, 1],
                'definition_levels' => [4, 4, 4],
                'values' => [1, 2, 3],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0, 0, 1],
                'definition_levels' => [4, 4, 4],
                'values' => ['a', 'b', 'c'],
            ],
        ],
    ])]
    public function test_optional_list_optional_struct_optional_int32_optional_string(array $rows, array $expectedColumnData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::structure(
                    [
                        Schema\FlatColumn::int32('int32'),
                        Schema\FlatColumn::string('string'),
                    ],
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.string')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedColumnData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            [
                'l' => [
                    ['s' => ['int32' => 1, 'string' => 'a']],
                    ['s' => ['int32' => 2, 'string' => 'b']],
                ],
            ],
        ],
        [
            'l.list.element.s.int32' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [5, 5],
                'values' => [1, 2],
            ],
            'l.list.element.s.string' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [5, 5],
                'values' => ['a', 'b'],
            ],
        ],
    ])]
    public function test_optional_list_optional_struct_optional_struct_optional_int32_optional_string(array $rows, array $expectedColumnData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::structure(
                    [
                        NestedColumn::struct('s', [
                            Schema\FlatColumn::int32('int32'),
                            Schema\FlatColumn::string('string'),
                        ]),
                    ],
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('l.list.element.s.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('l.list.element.s.string')->repetitions());

        self::assertEquals(5, $schema->get('l.list.element.s.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.s.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('l.list.element.s.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.s.string')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedColumnData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            ['l' => [1, 2, 3]],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [1, 1, 1],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_required_list_required_int32(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32(true), Repetition::REQUIRED));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element')->repetitions());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());
        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith([
        [
            ['l' => [[1, 2, 3, 4], [5, 6, 7, 8]]],
        ],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2, 2, 2, 1, 2, 2, 2],
                'definition_levels' => [2, 2, 2, 2, 2, 2, 2, 2],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8],
            ],
        ],
    ])]
    public function test_required_list_required_list_required_int32(array $rows, array $expectedFlatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::int32(true),
                    true
                ),
                Repetition::REQUIRED
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.list.element')->repetitions());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        /**
         * @var ?FlatColumnData $flatData
         */
        $flatData = \array_reduce(
            $rows,
            static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                ? $dremel->shred($schema->get('l'), $row)
                : $flatData->merge($dremel->shred($schema->get('l'), $row))
        );

        self::assertEquals($expectedFlatData, $flatData->normalize());

        self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
    }

    #[TestWith(
        [
            [
                ['l' => null],
            ],
            [],
            'Column "l" is required',
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [null]],
            ],
            [],
            'Column "l.list.element" is required',
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [['a' => null]]],
            ],
            [],
            'Column "l.list.element.key_value.value" is required',
        ]
    )]
    #[TestWith(
        [
            [
                ['l' => [['a' => 1]]],
            ],
            [
                'l.list.element.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'l.list.element.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => [1],
                ],
            ],
        ]
    )]
    public function test_required_list_required_map_string_required_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::map(
                    MapKey::string(),
                    MapValue::int32(true),
                    true
                )
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.value')->repetitions());

        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
        }
    }

    #[TestWith([
        [
            ['l' => null],
        ],
        [],
        'Column "l" is required',
    ])]
    #[TestWith([
        [
            ['l' => []],
        ],
        [],
        'Column "l.list.element" is required',
    ])]
    #[TestWith([
        [
            ['l' => [[]]],
        ],
        [],
        'Column "l.list.element.int32" is required',
    ])]
    #[TestWith([
        [
            ['l' => [['int32' => null]]],
        ],
        [],
        'Column "l.list.element.int32" is required',
    ])]
    #[TestWith([
        [
            ['l' => [['int32' => 1]]],
        ],
        [],
        'Column "l.list.element.string" is required',
    ])]
    #[TestWith([
        [
            ['l' => [['int32' => 1, 'string' => null]]],
        ],
        [],
        'Column "l.list.element.string" is required',
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    ['int32' => 1, 'string' => 'a'],
                    ['int32' => 2, 'string' => 'b'],
                ],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [1, 1],
                'values' => [1, 2],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [1, 1],
                'values' => ['a', 'b'],
            ],
        ],
    ])]
    public function test_required_list_required_struct_required_int32_required_string(array $rows, array $expectedFlatData, ?string $exceptMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::structure(
                    [
                        Schema\FlatColumn::int32('int32')->makeRequired(),
                        Schema\FlatColumn::string('string')->makeRequired(),
                    ],
                    true
                ),
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('l.list.element.string')->repetitions());

        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        if ($exceptMessage) {
            $this->expectExceptionMessage($exceptMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('l'), $row)
                    : $flatData->merge($dremel->shred($schema->get('l'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('l'), $flatData));
        }
    }
}
