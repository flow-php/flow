<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel;

use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Generator;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class DremelListsTest extends TestCase
{
    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_int32(array $rows, array $expectedColumnData): void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32()));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element')->repetitions());
        static::assertEquals(3, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedColumnData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_list_optional_int32(array $rows, array $expectedFlatData): void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::list(ListElement::int32())));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.list.element')->repetitions(),
        );
        static::assertEquals(5, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[TestWith([
        [
            [
                'l' => [ // level 0
                    [ // level 1
                        [ // level 2
                            'a' => 1, // level 3  // r: 0
                            'b' => 2, // r: 3
                        ],
                        [
                            'c' => 3, // r: 2
                        ],
                    ],
                    [
                        [
                            'd' => 4, // r: 1
                            'e' => 5, // r: 3
                        ],
                    ],
                    [
                        [
                            'f' => 6, // r: 1
                        ],
                        [
                            'g' => 7, // r: 2
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
    public function test_optional_list_optional_list_optional_map_string_optional_int32(
        array $rows,
        array $expectedFlatData,
    ): void {
        $schema = Schema::with(NestedColumn::list(
            'l',
            ListElement::list(ListElement::map(MapKey::string(), MapValue::int32())),
        ));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED',
            $schema->get('l.list.element.list.element.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.list.element.key_value.value')->repetitions(),
        );

        static::assertEquals(
            6,
            $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            3,
            $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxRepetitionLevel(),
        );

        static::assertEquals(
            7,
            $schema->get('l.list.element.list.element.key_value.value')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            3,
            $schema->get('l.list.element.list.element.key_value.value')->repetitions()->maxRepetitionLevel(),
        );

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[TestWith([
        [
            [
                'l' => [
                    [
                        ['a' => ['int32' => 1, 'string' => 'A', 'list' => [1, 2, 3], 'map' => ['AA' => 'value01']]],
                    ],
                    [
                        ['b' => ['int32' => 2, 'string' => 'B', 'list' => [4, 5, 6], 'map' => ['BB' => 'value02']]],
                        ['c' => ['int32' => 3, 'string' => 'C', 'list' => [7, 8, 9], 'map' => ['CC' => 'value03']]],
                    ],
                ],
            ],
        ],
        [
            'l.list.element.list.element.key_value.key' => [
                'repetition_levels' => [0, 1, 2],
                'definition_levels' => [6, 6, 6],
                'values' => ['a', 'b', 'c'],
            ],
            'l.list.element.list.element.key_value.value.int32' => [
                'repetition_levels' => [0, 1, 2],
                'definition_levels' => [8, 8, 8],
                'values' => [1, 2, 3],
            ],
            'l.list.element.list.element.key_value.value.string' => [
                'repetition_levels' => [0, 1, 2],
                'definition_levels' => [8, 8, 8],
                'values' => ['A', 'B', 'C'],
            ],
            'l.list.element.list.element.key_value.value.list.list.element' => [
                'repetition_levels' => [0, 4, 4, 1, 4, 4, 2, 4, 4],
                'definition_levels' => [10, 10, 10, 10, 10, 10, 10, 10, 10],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8, 9],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.key' => [
                'repetition_levels' => [0, 1, 2],
                'definition_levels' => [9, 9, 9],
                'values' => ['AA', 'BB', 'CC'],
            ],
            'l.list.element.list.element.key_value.value.map.key_value.value' => [
                'repetition_levels' => [0, 1, 2],
                'definition_levels' => [10, 10, 10],
                'values' => ['value01', 'value02', 'value03'],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_map_string_optional_struct_optional_int32_optional_list_optional_int32_optional_map_string_optional_string(
        array $rows,
        array $expectedFlatData,
    ): void {
        $schema = Schema::with(NestedColumn::list(
            'l',
            ListElement::list(ListElement::map(MapKey::string(), MapValue::structure([
                FlatColumn::int32('int32'),
                FlatColumn::string('string'),
                NestedColumn::list('list', ListElement::int32()),
                NestedColumn::map('map', MapKey::string(), MapValue::string()),
            ]))),
        ));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED',
            $schema->get('l.list.element.list.element.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.list.element.key_value.value.string')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.list.element.key_value.value.list.list.element')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,REQUIRED',
            $schema->get('l.list.element.list.element.key_value.value.map.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.list.element.key_value.value.map.key_value.value')->repetitions(),
        );

        static::assertEquals(
            6,
            $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            3,
            $schema->get('l.list.element.list.element.key_value.key')->repetitions()->maxRepetitionLevel(),
        );

        static::assertEquals(
            8,
            $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            3,
            $schema->get('l.list.element.list.element.key_value.value.int32')->repetitions()->maxRepetitionLevel(),
        );

        static::assertEquals(
            8,
            $schema->get('l.list.element.list.element.key_value.value.string')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            3,
            $schema->get('l.list.element.list.element.key_value.value.string')->repetitions()->maxRepetitionLevel(),
        );

        static::assertEquals(
            10,
            $schema
                ->get('l.list.element.list.element.key_value.value.list.list.element')
                ->repetitions()
                ->maxDefinitionLevel(),
        );
        static::assertEquals(
            4,
            $schema
                ->get('l.list.element.list.element.key_value.value.list.list.element')
                ->repetitions()
                ->maxRepetitionLevel(),
        );

        static::assertEquals(
            9,
            $schema
                ->get('l.list.element.list.element.key_value.value.map.key_value.key')
                ->repetitions()
                ->maxDefinitionLevel(),
        );
        static::assertEquals(
            4,
            $schema
                ->get('l.list.element.list.element.key_value.value.map.key_value.key')
                ->repetitions()
                ->maxRepetitionLevel(),
        );

        static::assertEquals(
            10,
            $schema
                ->get('l.list.element.list.element.key_value.value.map.key_value.value')
                ->repetitions()
                ->maxDefinitionLevel(),
        );
        static::assertEquals(
            4,
            $schema
                ->get('l.list.element.list.element.key_value.value.map.key_value.value')
                ->repetitions()
                ->maxRepetitionLevel(),
        );

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        $assembledRows = iterator_to_array((new DremelAssembler(
            DataConverter::initialize(Options::default()),
        ))->assemble($schema->get('l'), new ReadColumnData($schema->get('l'), $readFlatValues)));

        static::assertEquals(
            $rows,
            $assembledRows,
            'Expected rows: ' . json_encode($rows, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT) . "\n" . 'Actual rows: '
                . json_encode($assembledRows, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_list_optional_struct_optional_int32_optional_string(
        array $rows,
        array $expectedFlatData,
    ): void {
        $schema = Schema::with(NestedColumn::list(
            'l',
            ListElement::list(ListElement::structure([
                FlatColumn::int32('int32'),
                FlatColumn::string('string'),
            ])),
        ));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.list.element.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.list.element.string')->repetitions(),
        );

        static::assertEquals(6, $schema->get('l.list.element.list.element.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.list.element.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(
            6,
            $schema->get('l.list.element.list.element.string')->repetitions()->maxDefinitionLevel(),
        );
        static::assertEquals(
            2,
            $schema->get('l.list.element.list.element.string')->repetitions()->maxRepetitionLevel(),
        );

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
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
    ])]
    #[TestWith([
        [
            [
                'l' => [
                    ['a' => 1, 'b' => 2],
                    ['c' => 3, 'd' => 4],
                ],
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
    ])]
    public function test_optional_list_optional_map_string_optional_int32(
        array $rows,
        array $expectedFlatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(NestedColumn::list('l', ListElement::map(MapKey::string(), MapValue::int32())));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED',
            $schema->get('l.list.element.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.key_value.value')->repetitions(),
        );

        static::assertEquals(4, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        static::assertEquals(5, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, $rows);
        } else {
            $result = $shredder->shred($schema, $rows);

            $normalized = [];

            foreach ($result as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($expectedFlatData, $normalized);

            $readFlatValues = [];

            foreach ($result as $columnValues) {
                $readFlatValues[] = new ReadFlatColumnValues(
                    $columnValues->column,
                    (static function (array $values): Generator {
                        yield from $values;
                    })($columnValues->values()),
                    $columnValues->repetitionLevels(),
                    $columnValues->definitionLevels(),
                );
            }

            static::assertEquals(
                $rows,
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('l'),
                    new ReadColumnData($schema->get('l'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_struct_optional_int32_optional_list_string(
        array $rows,
        array $expectedColumnData,
    ): void {
        $schema = Schema::with(NestedColumn::list('l', ListElement::structure([
            FlatColumn::int32('int32'),
            NestedColumn::list('l', ListElement::string()),
        ])));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.l.list.element')->repetitions(),
        );

        static::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(6, $schema->get('l.list.element.l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.l.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        //        self::assertEquals($expectedColumnData, $normalized);
        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_struct_optional_int32_optional_map_int32_optional_string(
        array $rows,
        array $expectedColumnData,
    ): void {
        $schema = Schema::with(NestedColumn::list('l', ListElement::structure([
            FlatColumn::int32('int32'),
            NestedColumn::map('m', MapKey::int32(), MapValue::string()),
        ])));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,REQUIRED',
            $schema->get('l.list.element.m.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL',
            $schema->get('l.list.element.m.key_value.value')->repetitions(),
        );

        static::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(5, $schema->get('l.list.element.m.key_value.key')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.m.key_value.key')->repetitions()->maxRepetitionLevel());

        static::assertEquals(6, $schema->get('l.list.element.m.key_value.value')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.m.key_value.value')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedColumnData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_struct_optional_int32_optional_string(
        array $rows,
        array $expectedColumnData,
    ): void {
        $schema = Schema::with(NestedColumn::list('l', ListElement::structure([
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ])));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.string')->repetitions(),
        );

        static::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(4, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedColumnData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_optional_list_optional_struct_optional_struct_optional_int32_optional_string(
        array $rows,
        array $expectedColumnData,
    ): void {
        $schema = Schema::with(NestedColumn::list('l', ListElement::structure([
            NestedColumn::struct('s', [
                FlatColumn::int32('int32'),
                FlatColumn::string('string'),
            ]),
        ])));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.s.int32')->repetitions(),
        );
        static::assertEquals(
            'OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,OPTIONAL',
            $schema->get('l.list.element.s.string')->repetitions(),
        );

        static::assertEquals(5, $schema->get('l.list.element.s.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.s.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(5, $schema->get('l.list.element.s.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.s.string')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedColumnData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
            ['l' => [1, 2]],
        ],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [2, 2],
                'values' => [1, 2],
            ],
        ],
    ])]
    public function test_optional_list_required_int32(array $rows, array $expectedColumnData): void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32(true)));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element')->repetitions());
        static::assertEquals(2, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedColumnData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_required_list_required_int32(array $rows, array $expectedFlatData): void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32(true), Repetition::REQUIRED));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element')->repetitions());
        static::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_required_list_required_list_required_int32(array $rows, array $expectedFlatData): void
    {
        $schema = Schema::with(NestedColumn::list(
            'l',
            ListElement::list(ListElement::int32(true), true),
            Repetition::REQUIRED,
        ));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED',
            $schema->get('l.list.element.list.element')->repetitions(),
        );
        static::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        $result = $shredder->shred($schema, $rows);

        $normalized = [];

        foreach ($result as $flatPath => $columnValues) {
            $normalized[$flatPath] = [
                'repetition_levels' => $columnValues->repetitionLevels(),
                'definition_levels' => $columnValues->definitionLevels(),
                'values' => $columnValues->values(),
            ];
        }

        static::assertEquals($expectedFlatData, $normalized);

        $readFlatValues = [];

        foreach ($result as $columnValues) {
            $readFlatValues[] = new ReadFlatColumnValues(
                $columnValues->column,
                (static function (array $values): Generator {
                    yield from $values;
                })($columnValues->values()),
                $columnValues->repetitionLevels(),
                $columnValues->definitionLevels(),
            );
        }

        static::assertEquals(
            $rows,
            iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('l'),
                new ReadColumnData($schema->get('l'), $readFlatValues),
            )),
        );
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    #[TestWith([
        [
            ['l' => null],
        ],
        [],
        'Column "l" is required',
    ])]
    #[TestWith([
        [
            ['l' => [null]],
        ],
        [],
        'Column "l.list.element" is required',
    ])]
    #[TestWith([
        [
            ['l' => [['a' => null]]],
        ],
        [],
        'Column "l.list.element.key_value.value" is required',
    ])]
    #[TestWith([
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
    ])]
    public function test_required_list_required_map_string_required_int32(
        array $rows,
        array $expectedFlatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::list('l', ListElement::map(MapKey::string(), MapValue::int32(true), true))->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED',
            $schema->get('l.list.element.key_value.key')->repetitions(),
        );
        static::assertEquals(
            'REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED',
            $schema->get('l.list.element.key_value.value')->repetitions(),
        );

        static::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        static::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        static::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, $rows);
        } else {
            $result = $shredder->shred($schema, $rows);

            $normalized = [];

            foreach ($result as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($expectedFlatData, $normalized);

            $readFlatValues = [];

            foreach ($result as $columnValues) {
                $readFlatValues[] = new ReadFlatColumnValues(
                    $columnValues->column,
                    (static function (array $values): Generator {
                        yield from $values;
                    })($columnValues->values()),
                    $columnValues->repetitionLevels(),
                    $columnValues->definitionLevels(),
                );
            }

            static::assertEquals(
                $rows,
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('l'),
                    new ReadColumnData($schema->get('l'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
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
    public function test_required_list_required_struct_required_int32_required_string(
        array $rows,
        array $expectedFlatData,
        ?string $exceptMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::list('l', ListElement::structure([
                FlatColumn::int32('int32')->makeRequired(),
                FlatColumn::string('string')->makeRequired(),
            ], true))->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals(
            'REQUIRED,REPEATED,REQUIRED,REQUIRED',
            $schema->get('l.list.element.int32')->repetitions(),
        );
        static::assertEquals(
            'REQUIRED,REPEATED,REQUIRED,REQUIRED',
            $schema->get('l.list.element.string')->repetitions(),
        );

        static::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        if ($exceptMessage) {
            $this->expectExceptionMessage($exceptMessage);

            $shredder->shred($schema, $rows);
        } else {
            $result = $shredder->shred($schema, $rows);

            $normalized = [];

            foreach ($result as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($expectedFlatData, $normalized);

            $readFlatValues = [];

            foreach ($result as $columnValues) {
                $readFlatValues[] = new ReadFlatColumnValues(
                    $columnValues->column,
                    (static function (array $values): Generator {
                        yield from $values;
                    })($columnValues->values()),
                    $columnValues->repetitionLevels(),
                    $columnValues->definitionLevels(),
                );
            }

            static::assertEquals(
                $rows,
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('l'),
                    new ReadColumnData($schema->get('l'), $readFlatValues),
                )),
            );
        }
    }
}
