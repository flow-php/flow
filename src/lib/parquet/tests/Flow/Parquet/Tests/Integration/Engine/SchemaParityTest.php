<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType as C;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType as L;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType as P;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;

#[Group('native-extension')]
final class SchemaParityTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('arrow is not loaded');
        }
    }

    #[TestWith([PhpParquetEngine::class])]
    #[TestWith([RustParquetEngine::class])]
    public function test_every_type_is_written_with_one_physical_schema(string $engine): void
    {
        static::assertEquals(
            [
                ['boolean',                         P::BOOLEAN,              null, null,                null],
                ['int32',                           P::INT32,                null, null,                null],
                ['int64',                           P::INT64,                null, null,                null],
                ['float',                           P::FLOAT,                null, null,                null],
                ['double',                          P::DOUBLE,               null, null,                null],
                ['string',                          P::BYTE_ARRAY,           null, C::UTF8,             L::string()],
                ['json',                            P::BYTE_ARRAY,           null, C::JSON,             L::json()],
                ['enum',                            P::BYTE_ARRAY,           null, C::ENUM,             L::enum()],
                ['uuid',                            P::FIXED_LEN_BYTE_ARRAY, 16,   null,                L::uuid()],
                ['date',                            P::INT32,                null, C::DATE,             L::date()],
                ['datetime',                        P::INT64,                null, C::TIMESTAMP_MICROS, L::timestamp()],
                ['time',                            P::INT64,                null, C::TIME_MICROS,      L::time()],
                ['decimal',                         P::INT64,                null, C::DECIMAL,          L::decimal(
                    2,
                    10,
                )],
                ['decimal38',                       P::FIXED_LEN_BYTE_ARRAY, 16,   C::DECIMAL,          L::decimal(
                    9,
                    38,
                )],
                ['fixed4',                          P::FIXED_LEN_BYTE_ARRAY, 4,    null,                null],
                ['list_int.list.element',           P::INT64,                null, null,                null],
                ['list_datetime.list.element',      P::INT64,                null, C::TIMESTAMP_MICROS, L::timestamp()],
                ['map_string_date.key_value.key',   P::BYTE_ARRAY,           null, C::UTF8,             L::string()],
                ['map_string_date.key_value.value', P::INT32,                null, C::DATE,             L::date()],
                ['struct.id',                       P::FIXED_LEN_BYTE_ARRAY, 16,   null,                L::uuid()],
                ['struct.at',                       P::INT64,                null, C::TIME_MICROS,      L::time()],
                ['struct.amount',                   P::INT64,                null, C::DECIMAL,          L::decimal(
                    2,
                    10,
                )],
            ],
            array_map(
                static fn(FlatColumn $column): array => [
                    $column->flatPath(),
                    $column->type(),
                    $column->typeLength(),
                    $column->convertedType(),
                    $column->logicalType(),
                ],
                MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
                    new Writer(engine: ParquetEngineMother::create($engine)),
                    EveryType::schema(),
                    EveryType::rows(),
                ))
                    ->metadata()
                    ->schema()
                    ->columnsFlat(),
            ),
        );
    }

    #[TestWith([PhpParquetEngine::class])]
    #[TestWith([RustParquetEngine::class])]
    public function test_nested_enum_leaves_are_written_as_enum(string $engine): void
    {
        static::assertEquals(
            [
                ['list.list.element.colour',   C::ENUM, L::enum()],
                ['list.list.element.name',     C::UTF8, L::string()],
                ['map.key_value.key',          C::UTF8, L::string()],
                ['map.key_value.value.colour', C::ENUM, L::enum()],
                ['struct.colour',              C::ENUM, L::enum()],
                ['struct.name',                C::UTF8, L::string()],
            ],
            array_map(
                static fn(FlatColumn $column): array => [
                    $column->flatPath(),
                    $column->convertedType(),
                    $column->logicalType(),
                ],
                MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
                    new Writer(engine: ParquetEngineMother::create($engine)),
                    Schema::with(
                        NestedColumn::list('list', ListElement::structure([
                            FlatColumn::enum('colour'),
                            FlatColumn::string('name'),
                        ])),
                        NestedColumn::map('map', MapKey::string(), MapValue::structure([FlatColumn::enum('colour')])),
                        NestedColumn::struct('struct', [FlatColumn::enum('colour'), FlatColumn::string('name')]),
                    ),
                    [[
                        'list' => [['colour' => 'RED', 'name' => 'a']],
                        'map' => ['k' => ['colour' => 'GREEN']],
                        'struct' => ['colour' => 'BLUE', 'name' => 'b'],
                    ]],
                ))
                    ->metadata()
                    ->schema()
                    ->columnsFlat(),
            ),
        );
    }
}
