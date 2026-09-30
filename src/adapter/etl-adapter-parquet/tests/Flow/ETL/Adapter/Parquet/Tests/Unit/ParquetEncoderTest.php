<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ParquetEncoder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;

final class ParquetEncoderTest extends FlowTestCase
{
    public function test_columns_keeps_datetime_and_scalar_values(): void
    {
        $at = new DateTimeImmutable('2024-01-01 12:00:00 UTC');

        static::assertEquals(
            ['id' => [1], 'at' => [$at]],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::int64('id'),
                FlatColumn::datetime('at'),
            )))->columns(array_to_rows([['id' => 1, 'at' => $at]], schema(int_schema('id'), datetime_schema('at')))),
        );
    }

    public function test_columns_leaves_null_values_untouched(): void
    {
        static::assertSame(
            ['id' => [null], 'name' => [null]],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::int64('id'),
                FlatColumn::string('name'),
            )))->columns(array_to_rows(
                [['id' => null, 'name' => null]],
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            )),
        );
    }

    public function test_columns_renders_uuid_values_back_to_strings(): void
    {
        static::assertSame(
            ['id' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new ParquetEncoder(ParquetSchema::with(FlatColumn::uuid('id'))))->columns(array_to_rows([[
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]], schema(uuid_schema('id')))),
        );
    }

    public function test_columns_stringifies_json_and_uuid_nested_in_struct(): void
    {
        static::assertSame(
            ['body' => [['data' => '{"a":1}', 'id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'n' => 1]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('body', [
                FlatColumn::json('data'),
                FlatColumn::uuid('id'),
                FlatColumn::int64('n'),
            ]))))->columns(array_to_rows([[
                'body' => [
                    'data' => Json::fromArray(['a' => 1]),
                    'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                    'n' => 1,
                ],
            ]], schema(structure_schema('body', type_structure([
                'data' => type_json(),
                'id' => type_uuid(),
                'n' => type_integer(),
            ]))))),
        );
    }

    public function test_columns_stringifies_json_in_deep_struct(): void
    {
        static::assertSame(
            ['outer' => [['inner' => ['deep' => '{"e":5}']]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('outer', [NestedColumn::struct('inner', [FlatColumn::json(
                'deep',
            )])]))))->columns(array_to_rows([[
                'outer' => ['inner' => ['deep' => Json::fromArray(['e' => 5])]],
            ]], schema(structure_schema('outer', type_structure(['inner' => type_structure(['deep' => type_json()])]))))),
        );
    }

    public function test_columns_stringifies_uuid_list_elements(): void
    {
        static::assertSame(
            ['ids' => [['f47ac10b-58cc-4372-a567-0e02b2c3d479']]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::list(
                'ids',
                ListElement::uuid(),
            ))))->columns(array_to_rows([['ids' => [new Uuid(
                'f47ac10b-58cc-4372-a567-0e02b2c3d479',
            )]]], schema(list_schema('ids', type_list(type_uuid()))))),
        );
    }

    public function test_columns_stringifies_uuid_map_values_keeping_keys(): void
    {
        static::assertSame(
            ['ids' => [['a' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::map(
                'ids',
                MapKey::string(),
                MapValue::uuid(),
            ))))->columns(array_to_rows([['ids' => [
                'a' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]]], schema(map_schema('ids', type_map(type_string(), type_uuid()))))),
        );
    }

    public function test_columns_stringifies_uuids_in_list_of_structs(): void
    {
        static::assertSame(
            ['items' => [[['x' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::list('items', ListElement::structure([FlatColumn::uuid(
                'x',
            )])))))->columns(array_to_rows([[
                'items' => [['x' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]],
            ]], schema(list_schema('items', type_list(type_structure(['x' => type_uuid()])))))),
        );
    }

    public function test_columns_convert_only_the_non_null_values_of_a_converted_column(): void
    {
        static::assertSame(
            ['id' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479', null], 'n' => [1, 2]],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::uuid('id'),
                FlatColumn::int64('n'),
            )))->columns(array_to_rows(
                [['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'n' => 1], ['id' => null, 'n' => 2]],
                schema(uuid_schema('id', nullable: true), int_schema('n')),
            )),
        );
    }
}
