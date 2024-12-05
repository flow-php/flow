<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Dremel;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\ColumnDataValidator;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{DremelAssembler, DremelShredder, FlatColumnData};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DremelMapsTest extends TestCase
{
    #[TestWith(
        [
            [
                ['m' => null],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
                'm.key_value.value' => [
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
                ['m' => []],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
                'm.key_value.value' => [
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
                ['m' => [null]],
            ],
            [],
            'Column "m.key_value.key" is not string, got "integer" instead',
        ]
    )]
    #[TestWith(
        [
            [
                ['m' => ['a' => null]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value' => [
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
                ['m' => ['a' => 1]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [3],
                    'values' => [1],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['m' => ['a' => 1, 'b' => 2]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => ['a', 'b'],
                ],
                'm.key_value.value' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [3, 3],
                    'values' => [1, 2],
                ],
            ],
        ]
    )]
    public function test_optional_map_string_optional_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL', $schema->get('m.key_value.value')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(3, $schema->get('m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith(
        [
            [
                ['m' => null],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [0],
                    'values' => [],
                ],
                'm.key_value.value.list.element' => [
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
                ['m' => []],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
                'm.key_value.value.list.element' => [
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
                ['m' => [null]],
            ],
            [],
            'Column "m.key_value.key" is not string, got "integer" instead',
        ]
    )]
    #[TestWith(
        [
            [
                ['m' => ['a' => null]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value.list.element' => [
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
                ['m' => ['a' => null, 'b' => null]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => ['a', 'b'],
                ],
                'm.key_value.value.list.element' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['m' => ['a' => []]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value.list.element' => [
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
                ['m' => ['a' => [null]]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value.list.element' => [
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
                ['m' => ['a' => [null, null]]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [2],
                    'values' => ['a'],
                ],
                'm.key_value.value.list.element' => [
                    'repetition_levels' => [0, 2],
                    'definition_levels' => [4, 4],
                    'values' => [],
                ],
            ],
        ]
    )]
    #[TestWith(
        [
            [
                ['m' => ['a' => [1, 2], 'b' => [3, 4]]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => ['a', 'b'],
                ],
                'm.key_value.value.list.element' => [
                    'repetition_levels' => [0, 2, 1, 2],
                    'definition_levels' => [5, 5, 5, 5],
                    'values' => [1, 2, 3, 4],
                ],
            ],
        ]
    )]
    public function test_optional_map_string_optional_list_optional_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::list(ListElement::int32())
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('m.key_value.value.list.element')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('m.key_value.value.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            [
                'm' => null,
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [null],
            ],
        ],
        [],
        'Column "m.key_value.key" is not string, got "integer" instead',
    ])]
    #[TestWith([
        [
            [
                'm' => ['a' => null],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['a'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => ['a' => ['b' => null]],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['a'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => ['b'],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['c' => null],
                    'b' => [],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [2, 2],
                'values' => ['a', 'b'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 3],
                'values' => ['c'],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['c' => 1],
                    'b' => ['d' => 2],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [2, 2],
                'values' => ['a', 'b'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 4],
                'values' => ['c', 'd'],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [5, 5],
                'values' => [1, 2],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['c' => 1, 'd' => 2],
                    'b' => ['e' => 3, 'f' => 4],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [2, 2],
                'values' => ['a', 'b'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0, 2, 1, 2],
                'definition_levels' => [4, 4, 4, 4],
                'values' => ['c', 'd', 'e', 'f'],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0, 2, 1, 2],
                'definition_levels' => [5, 5, 5, 5],
                'values' => [1, 2, 3, 4],
            ],
        ],
    ])]
    public function test_optional_map_string_optional_map_string_optional_int(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::map(
                    MapKey::string(),
                    MapValue::int32()
                )
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.value.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('m.key_value.value.key_value.value')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('m.key_value.value.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('m.key_value.value.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            ['m' => null],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            ['m' => []],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['int32' => null, 'string' => null],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['a'],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['int32' => 1, 'string' => null],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['a'],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [1],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['int32' => 1, 'string' => 'b'],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['a'],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [1],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => ['b'],
            ],
        ],
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['int32' => 1, 'string' => 'b'],
                    'c' => ['int32' => 2, 'string' => 'd'],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [2, 2],
                'values' => ['a', 'c'],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 4],
                'values' => [1, 2],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 4],
                'values' => ['b', 'd'],
            ],
        ],
    ])]
    public function test_optional_map_string_optional_struct_optional_int32_optional_string(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::structure([
                    FlatColumn::int32('int32'),
                    FlatColumn::string('string'),
                ])
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('m.key_value.value.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('m.key_value.value.string')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('m.key_value.value.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('m.key_value.value.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value.string')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            [
                'm' => [
                    1 => [
                        'l' => [true, false],
                    ],
                    2 => null,
                ],
            ],
            [
                'm' => [
                    3 => [
                        'l' => null,
                    ],
                ],
            ],
            [
                'm' => [
                    4 => [
                        'l' => [],
                    ],
                ],
            ],
            [
                'm' => [
                    5 => [
                        'l' => [null],
                    ],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1, 0, 0, 0],
                'definition_levels' => [2, 2, 2, 2, 2],
                'values' => [1, 2, 3, 4, 5],
            ],
            'm.key_value.value.l.list.element' => [
                'repetition_levels' => [0, 2, 1, 0, 0, 0],
                'definition_levels' => [6, 6, 2, 3, 4, 5],
                'values' => [true, false],
            ],
        ],
    ])]
    public function test_optional_map_string_optional_struct_optional_list_optional_string(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::int32(),
                MapValue::structure([
                    NestedColumn::list(
                        'l',
                        ListElement::boolean()
                    ),
                ])
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('m.key_value.value.l.list.element')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(6, $schema->get('m.key_value.value.l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.l.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith(
        [
            [
                ['m' => []],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0],
                    'definition_levels' => [1],
                    'values' => [],
                ],
                'm.key_value.value' => [
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
                ['m' => ['a' => 1, 'b' => 2]],
            ],
            [
                'm.key_value.key' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => ['a', 'b'],
                ],
                'm.key_value.value' => [
                    'repetition_levels' => [0, 1],
                    'definition_levels' => [2, 2],
                    'values' => [1, 2],
                ],
            ],
        ]
    )]
    public function test_optional_map_string_required_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32(true)));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,REQUIRED', $schema->get('m.key_value.value')->repetitions());

        self::assertEquals(2, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );

            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            ['m' => null],
        ],
        [
        ],
        'Column "m" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => null]],
        ],
        [
        ],
        'Column "m.key_value.value" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => 1]],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => ['a'],
            ],
            'm.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [1],
            ],
        ],
    ])]
    public function test_required_map_string_required_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32(true))->makeRequired());

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.value')->repetitions());

        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            ['m' => null],
        ],
        [
        ],
        'Column "m" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => null]],
        ],
        [
        ],
        'Column "m.key_value.value" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => [1]]],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => ['a'],
            ],
            'm.key_value.value.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [1],
            ],
        ],
    ])]
    public function test_required_map_string_required_list_required_int32(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::list(
                    ListElement::int32(true),
                    true
                )
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.value.list.element')->repetitions());

        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('m.key_value.value.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            [
                'm' => null,
            ],
        ],
        [
        ],
        'Column "m" is required',
    ])]
    #[TestWith([
        [
            [
                'm' => ['a' => null],
            ],
        ],
        [
        ],
        'Column "m.key_value.value" is required',
    ])]
    #[TestWith([
        [
            [
                'm' => ['a' => ['b' => null]],
            ],
        ],
        [
        ],
        'Column "m.key_value.value.key_value.value" is required',
    ])]
    #[TestWith([
        [
            [
                'm' => ['a' => ['b' => 1]],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => ['a'],
            ],
            'm.key_value.value.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['b'],
            ],
            'm.key_value.value.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [1],
            ],
        ],
    ])]
    public function test_required_map_string_required_map_string_required_int(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::map(
                    MapKey::string(),
                    MapValue::int32(true),
                    true
                )
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.value.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.value.key_value.value')->repetitions());

        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('m.key_value.value.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('m.key_value.value.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('m.key_value.value.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );

        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }

    #[TestWith([
        [
            ['m' => null],
        ],
        [
        ],
        'Column "m" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => null]],
        ],
        [
        ],
        'Column "m.key_value.value" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => []]],
        ],
        [
        ],
        'Column "m.key_value.value.int32" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => ['int32' => null]]],
        ],
        [
        ],
        'Column "m.key_value.value.int32" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => ['int32' => 1]]],
        ],
        [
        ],
        'Column "m.key_value.value.string" is required',
    ])]
    #[TestWith([
        [
            ['m' => ['a' => ['int32' => 1, 'string' => null]]],
        ],
        [
        ],
        'Column "m.key_value.value.string" is required',
    ])]
    #[TestWith([
        [
            [
                'm' => [
                    'a' => ['int32' => 1, 'string' => 'b'],
                    'c' => ['int32' => 2, 'string' => 'd'],
                ],
            ],
        ],
        [
            'm.key_value.key' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [1, 1],
                'values' => ['a', 'c'],
            ],
            'm.key_value.value.int32' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [1, 1],
                'values' => [1, 2],
            ],
            'm.key_value.value.string' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [1, 1],
                'values' => ['b', 'd'],
            ],
        ],
    ])]
    public function test_required_map_string_required_struct_required_int32_required_string(array $rows, array $expectedFlatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::map(
                'm',
                MapKey::string(),
                MapValue::structure(
                    [
                        FlatColumn::int32('int32')->makeRequired(),
                        FlatColumn::string('string')->makeRequired(),
                    ],
                    true
                )
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('m.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('m.key_value.value.int32')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('m.key_value.value.string')->repetitions());

        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('m.key_value.value.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('m.key_value.value.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('m.key_value.value.string')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
        } else {
            /**
             * @var ?FlatColumnData $flatData
             */
            $flatData = \array_reduce(
                $rows,
                static fn (?FlatColumnData $flatData, array $row) => $flatData === null
                    ? $dremel->shred($schema->get('m'), $row)
                    : $flatData->merge($dremel->shred($schema->get('m'), $row))
            );
            self::assertEquals($expectedFlatData, $flatData->normalize());
            self::assertEquals($rows, (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('m'), $flatData));
        }
    }
}
