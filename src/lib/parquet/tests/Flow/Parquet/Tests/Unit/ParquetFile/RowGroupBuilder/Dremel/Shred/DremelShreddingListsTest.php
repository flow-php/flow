<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Dremel\Shred;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Dremel;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\ColumnDataValidator;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{ListElement, MapKey, MapValue, NestedColumn, Repetition};
use PHPUnit\Framework\Attributes\{TestWith};
use PHPUnit\Framework\TestCase;

final class DremelShreddingListsTest extends TestCase
{
    #[TestWith([
        ['l' => null],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => []],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [null]],
        [
            'l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [1, 2, 3]],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [3, 3, 3],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_optional_list_optional_int32(array $row, array $flatData) : void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32()));

        $dremel = new Dremel(new ColumnDataValidator());

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element')->repetitions());
        self::assertEquals(3, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        self::assertEquals(
            $flatData,
            $dremel->shredRow($schema->get('l'), $row)->normalize()
        );
    }

    #[TestWith([
        ['l' => null],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => []],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [null]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[null]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [4],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[1]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [5],
                'values' => [1],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[1, 2]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [5, 5],
                'values' => [1, 2],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[1, null]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2],
                'definition_levels' => [5, 4],
                'values' => [1],
            ],
        ],
    ])]
    #[TestWith([
        ['l' => [[1, 2, 3, 4], [5, 6, 7, 8]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2, 2, 2, 1, 2, 2, 2],
                'definition_levels' => [5, 5, 5, 5, 5, 5, 5, 5],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8],
            ],
        ],
    ])]
    public function test_optional_list_optional_list_optional_int32(array $row, array $flatData) : void
    {
        $schema = Schema::with(
            NestedColumn::list(
                'l',
                ListElement::list(
                    ListElement::int32()
                )
            )
        );

        $dremel = new Dremel(new ColumnDataValidator());

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.list.element')->repetitions());
        self::assertEquals(5, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        self::assertEquals(
            $flatData,
            $dremel->shredRow($schema->get('l'), $row)->normalize()
        );
    }

    #[TestWith(
        [
            ['l' => null],
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
            ['l' => []],
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
            ['l' => null],
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
            ['l' => []],
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
            ['l' => [null]],
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
            ['l' => [[]]],
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
            ['l' => [['a' => null]]],
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
            ['l' => [['a' => 1]]],
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
            ['l' => [['a' => 1, 'b' => 2], ['c' => 3, 'd' => 4]]],
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
    public function test_optional_list_optional_map_string_optional_int32(array $row, array $flatData, ?string $exceptionMessage = null) : void
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

        $dremel = new Dremel(new ColumnDataValidator());

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,REPEATED,OPTIONAL', $schema->get('l.list.element.key_value.value')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(5, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shredRow($schema->get('l'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shredRow($schema->get('l'), $row)->normalize()
            );
        }
    }

    #[TestWith([
        ['l' => null],
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
        ['l' => []],
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
        ['l' => [[]]],
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
        ['l' => [['int32' => 1, 'string' => 'a']]],
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
            'l' => [
                ['int32' => 1, 'string' => 'a'],
                ['int32' => 2, 'string' => 'b'],
            ],
        ],
        [
            'l.list.element.int32' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 4],
                'values' => [1, 2],
            ],
            'l.list.element.string' => [
                'repetition_levels' => [0, 1],
                'definition_levels' => [4, 4],
                'values' => ['a', 'b'],
            ],
        ],
    ])]
    public function test_optional_list_optional_struct_optional_int32_optional_string(array $row, array $flatData) : void
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

        $dremel = new Dremel(new ColumnDataValidator());

        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('OPTIONAL,REPEATED,OPTIONAL,OPTIONAL', $schema->get('l.list.element.string')->repetitions());

        self::assertEquals(4, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        self::assertEquals(
            $flatData,
            $dremel->shredRow($schema->get('l'), $row)->normalize()
        );
    }

    #[TestWith([
        ['l' => [1, 2, 3]],
        [
            'l.list.element' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [1, 1, 1],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_required_list_required_int32(array $row, array $flatData) : void
    {
        $schema = Schema::with(NestedColumn::list('l', ListElement::int32(true), Repetition::REQUIRED));

        $dremel = new Dremel(new ColumnDataValidator());

        self::assertEquals('REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element')->repetitions());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element')->repetitions()->maxRepetitionLevel());

        self::assertEquals(
            $flatData,
            $dremel->shredRow($schema->get('l'), $row)->normalize()
        );
    }

    #[TestWith([
        ['l' => [[1, 2, 3, 4], [5, 6, 7, 8]]],
        [
            'l.list.element.list.element' => [
                'repetition_levels' => [0, 2, 2, 2, 1, 2, 2, 2],
                'definition_levels' => [2, 2, 2, 2, 2, 2, 2, 2],
                'values' => [1, 2, 3, 4, 5, 6, 7, 8],
            ],
        ],
    ])]
    public function test_required_list_required_list_required_int32(array $row, array $flatData) : void
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

        $flattener = new Dremel(new ColumnDataValidator());

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.list.element')->repetitions());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.list.element')->repetitions()->maxRepetitionLevel());

        self::assertEquals(
            $flatData,
            $flattener->shredRow($schema->get('l'), $row)->normalize()
        );
    }

    #[TestWith(
        [
            ['l' => null],
            [],
            'Column "l" is required',
        ]
    )]
    #[TestWith(
        [
            ['l' => [null]],
            [],
            'Column "element" is required',
        ]
    )]
    #[TestWith(
        [
            ['l' => [['a' => null]]],
            [],
            'Column "value" is required',
        ]
    )]
    #[TestWith(
        [
            ['l' => [['a' => 1]]],
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
    public function test_required_list_required_map_string_required_int32(array $row, array $flatData, ?string $exceptionMessage = null) : void
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

        $flattener = new Dremel(new ColumnDataValidator());

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REPEATED,REQUIRED', $schema->get('l.list.element.key_value.value')->repetitions());

        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(2, $schema->get('l.list.element.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $flattener->shredRow($schema->get('l'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $flattener->shredRow($schema->get('l'), $row)->normalize()
            );
        }
    }

    #[TestWith([
        ['l' => null],
        [],
        'Column "l" is required',
    ])]
    #[TestWith([
        ['l' => []],
        [],
        'Column "int32" is required',
    ])]
    #[TestWith([
        ['l' => [[]]],
        [],
        'Column "int32" is required',
    ])]
    #[TestWith([
        ['l' => [['int32' => null]]],
        [],
        'Column "int32" is required',
    ])]
    #[TestWith([
        ['l' => [['int32' => 1]]],
        [],
        'Column "string" is required',
    ])]
    #[TestWith([
        ['l' => [['int32' => 1, 'string' => null]]],
        [],
        'Column "string" is required',
    ])]
    #[TestWith([
        [
            'l' => [
                ['int32' => 1, 'string' => 'a'],
                ['int32' => 2, 'string' => 'b'],
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
    public function test_required_list_required_struct_required_int32_required_string(array $row, array $flatData, ?string $exceptMessage = null) : void
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

        $flattener = new Dremel(new ColumnDataValidator());

        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('l.list.element.int32')->repetitions());
        self::assertEquals('REQUIRED,REPEATED,REQUIRED,REQUIRED', $schema->get('l.list.element.string')->repetitions());

        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('l.list.element.string')->repetitions()->maxRepetitionLevel());

        if ($exceptMessage) {
            $this->expectExceptionMessage($exceptMessage);
            $flattener->shredRow($schema->get('l'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $flattener->shredRow($schema->get('l'), $row)->normalize()
            );
        }
    }
}
