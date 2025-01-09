<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Dremel;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\ColumnDataValidator;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{DremelAssembler, DremelShredder};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\ParquetFile\Schema\{MapKey, MapValue};
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DremelStructuresTest extends TestCase
{
    #[TestWith([
        [
            's' => null,
        ],
        [
            's.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            's.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            's.bool' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'int32' => null,
                'string' => null,
                'bool' => null,
            ],
        ],
        [
            's.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            's.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            's.bool' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'int32' => 1,
                'string' => 'string',
                'bool' => true,
            ],
        ],
        [
            's.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [1],
            ],
            's.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => ['string'],
            ],
            's.bool' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [true],
            ],
        ],
    ])]
    public function test_optional_struct_optional_int_optional_string_optional_bool(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    FlatColumn::int32('int32'),
                    FlatColumn::string('string'),
                    FlatColumn::boolean('bool'),
                ]
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.int32')->repetitions());
        self::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.string')->repetitions());
        self::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.bool')->repetitions());

        self::assertEquals(2, $schema->get('s.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('s.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.string')->repetitions()->maxRepetitionLevel());

        self::assertEquals(2, $schema->get('s.bool')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.bool')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'l' => null,
            ],
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'l' => [],
            ],
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'l' => [null],
            ],
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'l' => [1, 2, 3],
            ],
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [4, 4, 4],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_optional_struct_optional_list_optional_int(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::list('l', ListElement::int32()),
                ]
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('s.l.list.element')->repetitions());

        self::assertEquals(4, $schema->get('s.l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'm' => null,
            ],
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'm' => [],
            ],
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'm' => ['a' => null],
            ],
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => ['a'],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                'm' => ['a' => 1, 'b' => 2, 'c' => 3],
            ],
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [3, 3, 3],
                'values' => ['a', 'b', 'c'],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [4, 4, 4],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_optional_struct_optional_map_string_optional_int(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::map('m', MapKey::string(), MapValue::int32()),
                ]
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,OPTIONAL,REPEATED,REQUIRED', $schema->get('s.m.key_value.key')->repetitions());
        self::assertEquals('OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('s.m.key_value.value')->repetitions());

        self::assertEquals(3, $schema->get('s.m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(4, $schema->get('s.m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
            's.s1.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
            's.s1.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                's1' => null,
            ],
        ],
        [
            's.s1.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
            's.s1.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                's1' => ['int32' => null, 'string' => null],
            ],
        ],
        [
            's.s1.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
            's.s1.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [2],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        [
            's' => [
                's1' => ['int32' => 1, 'string' => 'string'],
            ],
        ],
        [
            's.s1.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => [1],
            ],
            's.s1.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [3],
                'values' => ['string'],
            ],
        ],
    ])]
    public function test_optional_struct_optional_struct_optional_int32_optional_string(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::struct(
                        's1',
                        [
                            FlatColumn::int32('int32'),
                            FlatColumn::string('string'),
                        ]
                    ),
                ]
            )
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('s.s1.int32')->repetitions());
        self::assertEquals('OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('s.s1.string')->repetitions());

        self::assertEquals(3, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(3, $schema->get('s.s1.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.s1.string')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
        ],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [
        ],
        'Column "s.int32" is required',
    ])]
    #[TestWith([
        [
            's' => ['int32' => 1],
        ],
        [
        ],
        'Column "s.string" is required',
    ])]
    #[TestWith([
        [
            's' => ['int32' => 1, 'string' => 'a'],
        ],
        [
        ],
        'Column "s.bool" is required',
    ])]
    #[TestWith([
        [
            's' => [
                'int32' => 1,
                'string' => 'string',
                'bool' => true,
            ],
        ],
        [
            's.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [1],
            ],
            's.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => ['string'],
            ],
            's.bool' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [true],
            ],
        ],
    ])]
    public function test_required_struct_required_int_required_string_required_bool(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    FlatColumn::int32('int32')->makeRequired(),
                    FlatColumn::string('string')->makeRequired(),
                    FlatColumn::boolean('bool')->makeRequired(),
                ]
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REQUIRED', $schema->get('s.int32')->repetitions());
        self::assertEquals('REQUIRED,REQUIRED', $schema->get('s.string')->repetitions());
        self::assertEquals('REQUIRED,REQUIRED', $schema->get('s.bool')->repetitions());

        self::assertEquals(0, $schema->get('s.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(0, $schema->get('s.string')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.string')->repetitions()->maxRepetitionLevel());

        self::assertEquals(0, $schema->get('s.bool')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.bool')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
        ],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [
        ],
        'Column "s.l" is required',
    ])]
    #[TestWith([
        [
            's' => ['l' => null],
        ],
        [
        ],
        'Column "s.l" is required',
    ])]
    #[TestWith([
        [
            's' => ['l' => [null]],
        ],
        [
        ],
        'Column "s.l.list.element" is required',
    ])]
    #[TestWith([
        [
            's' => ['l' => [1, 2, 3]],
        ],
        [
            's.l.list.element' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [1, 1, 1],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_required_struct_required_list_required_int(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::list('l', ListElement::int32(true))->makeRequired(),
                ]
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.l.list.element')->repetitions());

        self::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
        ],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [
        ],
        'Column "s.m" is required',
    ])]
    #[TestWith([
        [
            's' => ['m' => null],
        ],
        [
        ],
        'Column "s.m" is required',
    ])]
    #[TestWith([
        [
            's' => ['m' => ['a' => null]],
        ],
        [
        ],
        'Column "s.m.key_value.value" is required',
    ])]
    #[TestWith([
        [
            's' => [
                'm' => [
                    'a' => 1,
                    'b' => 2,
                    'c' => 3,
                ],
            ],
        ],
        [
            's.m.key_value.key' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [1, 1, 1],
                'values' => ['a', 'b', 'c'],
            ],
            's.m.key_value.value' => [
                'repetition_levels' => [0, 1, 1],
                'definition_levels' => [1, 1, 1],
                'values' => [1, 2, 3],
            ],
        ],
    ])]
    public function test_required_struct_required_map_string_required_int(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::map('m', MapKey::string(), MapValue::int32(true))->makeRequired(),
                ]
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.m.key_value.key')->repetitions());
        self::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.m.key_value.value')->repetitions());

        self::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxRepetitionLevel());

        self::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxDefinitionLevel());
        self::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }

    #[TestWith([
        [
            's' => null,
        ],
        [
        ],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [
        ],
        'Column "s.s1" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => null],
        ],
        [
        ],
        'Column "s.s1" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => ['int32' => null, 'string' => null]],
        ],
        [
        ],
        'Column "s.s1.int32" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => ['int32' => 1, 'string' => null]],
        ],
        [
        ],
        'Column "s.s1.string" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => ['int32' => 1, 'string' => 'string']],
        ],
        [
            's.s1.int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [1],
            ],
            's.s1.string' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => ['string'],
            ],
        ],
    ])]
    public function test_required_struct_required_struct_required_int32_required_string(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                's',
                [
                    NestedColumn::struct(
                        's1',
                        [
                            FlatColumn::int32('int32')->makeRequired(),
                            FlatColumn::string('string')->makeRequired(),
                        ]
                    )->makeRequired(),
                ]
            )->makeRequired()
        );

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED,REQUIRED,REQUIRED', $schema->get('s.s1.int32')->repetitions());
        self::assertEquals('REQUIRED,REQUIRED,REQUIRED', $schema->get('s.s1.string')->repetitions());

        self::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        self::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('s'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('s'), $row)->normalize()
            );
            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('s'), $dremel->shred($schema->get('s'), $row))
            );
        }
    }
}
