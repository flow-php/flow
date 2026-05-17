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
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DremelFlatTest extends TestCase
{
    #[TestWith([
        ['int32' => null],
        [
            'int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [],
            ],
        ],
    ])]
    #[TestWith([
        ['int32' => 1],
        [
            'int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [1],
                'values' => [1],
            ],
        ],
    ])]
    public function test_optional_int32(array $row, array $flatData): void
    {
        $schema = Schema::with(FlatColumn::int32('int32'));

        static::assertEquals('OPTIONAL', $schema->get('int32')->repetitions());
        static::assertEquals(1, $schema->get('int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('int32')->repetitions()->maxRepetitionLevel());

        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $narrowedRow = \Flow\Types\DSL\type_map(\Flow\Types\DSL\type_string(), \Flow\Types\DSL\type_mixed())->assert(
            $row,
        );

        $shredResult = $shredder->shred($schema, [$narrowedRow]);

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
            \iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                $schema->get('int32'),
                new ReadColumnData($schema->get('int32'), $readFlatValues),
            )),
        );
    }

    #[TestWith([
        ['int32' => 1],
        [
            'int32' => [
                'repetition_levels' => [0],
                'definition_levels' => [0],
                'values' => [1],
            ],
        ],
    ])]
    #[TestWith([[], [], 'Column "int32" is required'])]
    public function test_required_int32(array $row, array $flatData, ?string $exceptionMessage = null): void
    {
        $schema = Schema::with(FlatColumn::int32('int32')->makeRequired());
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        static::assertEquals('REQUIRED', $schema->get('int32')->repetitions());
        static::assertEquals(0, $schema->get('int32')->repetitions()->maxDefinitionLevel());
        static::assertEquals(0, $schema->get('int32')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $narrowedRow = \Flow\Types\DSL\type_map(
                \Flow\Types\DSL\type_string(),
                \Flow\Types\DSL\type_mixed(),
            )->assert($row);

            $shredder->shred($schema, [$narrowedRow]);
        } else {
            $narrowedRow = \Flow\Types\DSL\type_map(
                \Flow\Types\DSL\type_string(),
                \Flow\Types\DSL\type_mixed(),
            )->assert($row);

            $shredResult = $shredder->shred($schema, [$narrowedRow]);

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
                \iterator_to_array((new DremelAssembler(DataConverter::initialize(Options::default())))->assemble(
                    $schema->get('int32'),
                    new ReadColumnData($schema->get('int32'), $readFlatValues),
                )),
            );
        }
    }
}
