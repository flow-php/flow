<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Dremel;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\ColumnDataValidator;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{DremelAssembler, DremelShredder};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn};
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
    public function test_optional_int32(array $row, array $flatData) : void
    {
        $schema = Schema::with(FlatColumn::int32('int32'));

        self::assertEquals('OPTIONAL', $schema->get('int32')->repetitions());
        self::assertEquals(1, $schema->get('int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('int32')->repetitions()->maxRepetitionLevel());

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals(
            $flatData,
            $dremel->shred($schema->get('int32'), $row)->normalize()
        );

        self::assertEquals(
            [
                $row,
            ],
            (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('int32'), $dremel->shred($schema->get('int32'), $row))
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
    public function test_required_int32(array $row, array $flatData, ?string $exceptionMessage = null) : void
    {
        $schema = Schema::with(FlatColumn::int32('int32')->makeRequired());
        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        self::assertEquals('REQUIRED', $schema->get('int32')->repetitions());
        self::assertEquals(0, $schema->get('int32')->repetitions()->maxDefinitionLevel());
        self::assertEquals(0, $schema->get('int32')->repetitions()->maxRepetitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $dremel->shred($schema->get('int32'), $row);
        } else {
            self::assertEquals(
                $flatData,
                $dremel->shred($schema->get('int32'), $row)->normalize()
            );

            self::assertEquals(
                [
                    $row,
                ],
                (new DremelAssembler(DataConverter::initialize(Options::default())))->assemble($schema->get('int32'), $dremel->shred($schema->get('int32'), $row))
            );
        }
    }
}
