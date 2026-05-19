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
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class DremelStructuresTest extends TestCase
{
    /**
     * @param array<string, mixed> $row
     */
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
    public function test_optional_struct_optional_int_optional_string_optional_bool(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(NestedColumn::struct('s', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
            FlatColumn::boolean('bool'),
        ]));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.int32')->repetitions());
        static::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.string')->repetitions());
        static::assertEquals('OPTIONAL,OPTIONAL', $schema->get('s.bool')->repetitions());

        static::assertEquals(2, $schema->get('s.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(2, $schema->get('s.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.string')->repetitions()->maxRepetitionLevel());

        static::assertEquals(2, $schema->get('s.bool')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.bool')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
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
    public function test_optional_struct_optional_list_optional_int(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(NestedColumn::struct('s', [
            NestedColumn::list('l', ListElement::int32()),
        ]));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('s.l.list.element')->repetitions());

        static::assertEquals(4, $schema->get('s.l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
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
    public function test_optional_struct_optional_map_string_optional_int(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(NestedColumn::struct('s', [
            NestedColumn::map('m', MapKey::string(), MapValue::int32()),
        ]));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,OPTIONAL,REPEATED,REQUIRED', $schema->get('s.m.key_value.key')->repetitions());
        static::assertEquals('OPTIONAL,OPTIONAL,REPEATED,OPTIONAL', $schema->get('s.m.key_value.value')->repetitions());

        static::assertEquals(3, $schema->get('s.m.key_value.key')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxRepetitionLevel());

        static::assertEquals(4, $schema->get('s.m.key_value.value')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
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
    public function test_optional_struct_optional_struct_optional_int32_optional_string(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(NestedColumn::struct('s', [
            NestedColumn::struct('s1', [
                FlatColumn::int32('int32'),
                FlatColumn::string('string'),
            ]),
        ]));

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('s.s1.int32')->repetitions());
        static::assertEquals('OPTIONAL,OPTIONAL,OPTIONAL', $schema->get('s.s1.string')->repetitions());

        static::assertEquals(3, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(3, $schema->get('s.s1.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.s1.string')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
    #[TestWith([
        [
            's' => null,
        ],
        [],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [],
        'Column "s.int32" is required',
    ])]
    #[TestWith([
        [
            's' => ['int32' => 1],
        ],
        [],
        'Column "s.string" is required',
    ])]
    #[TestWith([
        [
            's' => ['int32' => 1, 'string' => 'a'],
        ],
        [],
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
    public function test_required_struct_required_int_required_string_required_bool(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::struct('s', [
                FlatColumn::int32('int32')->makeRequired(),
                FlatColumn::string('string')->makeRequired(),
                FlatColumn::boolean('bool')->makeRequired(),
            ])->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED,REQUIRED', $schema->get('s.int32')->repetitions());
        static::assertEquals('REQUIRED,REQUIRED', $schema->get('s.string')->repetitions());
        static::assertEquals('REQUIRED,REQUIRED', $schema->get('s.bool')->repetitions());

        static::assertEquals(0, $schema->get('s.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(0, $schema->get('s.string')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.string')->repetitions()->maxRepetitionLevel());

        static::assertEquals(0, $schema->get('s.bool')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.bool')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
    #[TestWith([
        [
            's' => null,
        ],
        [],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [],
        'Column "s.l" is required',
    ])]
    #[TestWith([
        [
            's' => ['l' => null],
        ],
        [],
        'Column "s.l" is required',
    ])]
    #[TestWith([
        [
            's' => ['l' => [null]],
        ],
        [],
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
    public function test_required_struct_required_list_required_int(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::struct('s', [
                NestedColumn::list('l', ListElement::int32(true))->makeRequired(),
            ])->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.l.list.element')->repetitions());

        static::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.l.list.element')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
    #[TestWith([
        [
            's' => null,
        ],
        [],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [],
        'Column "s.m" is required',
    ])]
    #[TestWith([
        [
            's' => ['m' => null],
        ],
        [],
        'Column "s.m" is required',
    ])]
    #[TestWith([
        [
            's' => ['m' => ['a' => null]],
        ],
        [],
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
    public function test_required_struct_required_map_string_required_int(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::struct('s', [
                NestedColumn::map('m', MapKey::string(), MapValue::int32(true))->makeRequired(),
            ])->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.m.key_value.key')->repetitions());
        static::assertEquals('REQUIRED,REQUIRED,REPEATED,REQUIRED', $schema->get('s.m.key_value.value')->repetitions());

        static::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.m.key_value.key')->repetitions()->maxRepetitionLevel());

        static::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxDefinitionLevel());
        static::assertEquals(1, $schema->get('s.m.key_value.value')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }

    /**
     * @param array<string, mixed> $row
     */
    #[TestWith([
        [
            's' => null,
        ],
        [],
        'Column "s" is required',
    ])]
    #[TestWith([
        [
            's' => [],
        ],
        [],
        'Column "s.s1" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => null],
        ],
        [],
        'Column "s.s1" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => ['int32' => null, 'string' => null]],
        ],
        [],
        'Column "s.s1.int32" is required',
    ])]
    #[TestWith([
        [
            's' => ['s1' => ['int32' => 1, 'string' => null]],
        ],
        [],
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
    public function test_required_struct_required_struct_required_int32_required_string(
        array $row,
        array $flatData,
        ?string $exceptionMessage = null,
    ): void {
        $schema = Schema::with(
            NestedColumn::struct('s', [
                NestedColumn::struct('s1', [
                    FlatColumn::int32('int32')->makeRequired(),
                    FlatColumn::string('string')->makeRequired(),
                ])->makeRequired(),
            ])->makeRequired(),
        );

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED,REQUIRED,REQUIRED', $schema->get('s.s1.int32')->repetitions());
        static::assertEquals('REQUIRED,REQUIRED,REQUIRED', $schema->get('s.s1.string')->repetitions());

        static::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        static::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('s.s1.int32')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);

            $shredder->shred($schema, [$row]);
        } else {
            $shredResult = $shredder->shred($schema, [$row]);

            $normalized = [];

            foreach ($shredResult as $flatPath => $columnValues) {
                $normalized[$flatPath] = [
                    'repetition_levels' => $columnValues->repetitionLevels(),
                    'definition_levels' => $columnValues->definitionLevels(),
                    'values' => $columnValues->values(),
                ];
            }

            static::assertEquals($flatData, $normalized);

            $readFlatValues = [];

            foreach ($shredResult as $flatValue) {
                $valuesGenerator = (static function () use ($flatValue) {
                    foreach ($flatValue->values() as $value) {
                        yield $value;
                    }
                })();
                $readFlatValues[] = new ReadFlatColumnValues(
                    $flatValue->column,
                    $valuesGenerator,
                    $flatValue->repetitionLevels(),
                    $flatValue->definitionLevels(),
                );
            }

            static::assertEquals(
                [
                    $row,
                ],
                iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('s'),
                    new ReadColumnData($schema->get('s'), $readFlatValues),
                )),
            );
        }
    }
}
