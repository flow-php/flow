<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ParquetEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;

final class ParquetEncoderTest extends FlowTestCase
{
    public function test_decode_leaves_null_logical_values_untouched(): void
    {
        static::assertSame(
            ['id' => null, 'payload' => null],
            (new ParquetEncoder(ParquetSchema::with(FlatColumn::uuid('id'), FlatColumn::json('payload'))))->decode([[
                'id' => null,
                'payload' => null,
            ]])[0]->values,
        );
    }

    public function test_decode_normalizes_json_columns_to_flow_native(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(FlatColumn::json('payload'))))->decode([[
            'payload' => '{"id":1,"status":"NEW"}',
        ]]);

        static::assertInstanceOf(Json::class, $decoded[0]->values['payload']);
        static::assertSame(['id' => 1, 'status' => 'NEW'], $decoded[0]->values['payload']->toArray());
    }

    public function test_decode_normalizes_uuid_columns_to_flow_native(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(FlatColumn::uuid('id'))))->decode([[
            'id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
        ]]);

        static::assertInstanceOf(Uuid::class, $decoded[0]->values['id']);
        static::assertSame('f47ac10b-58cc-4372-a567-0e02b2c3d479', $decoded[0]->values['id']->toString());
    }

    public function test_decode_passes_scalar_value_maps_through_unchanged(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            (new ParquetEncoder(ParquetSchema::with(FlatColumn::int64('id'), FlatColumn::string('name'))))->decode([[
                'id' => 1,
                'name' => 'Alice',
            ]])[0]->values,
        );
    }

    public function test_encode_coerces_values_to_the_column_type(): void
    {
        static::assertSame(
            [['id' => '1', 'name' => 'test']],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::string('id'),
                FlatColumn::string('name'),
            )))->encode([new TypedRowValues(['id' => 1, 'name' => 'test'], [
                'id' => type_integer(),
                'name' => type_string(),
            ])]),
        );
    }

    public function test_encode_keeps_datetime_and_scalar_values(): void
    {
        $at = new DateTimeImmutable('2024-01-01 12:00:00 UTC');

        static::assertSame(
            [['id' => 1, 'at' => $at]],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::int64('id'),
                FlatColumn::datetime('at'),
            )))->encode([new TypedRowValues(['id' => 1, 'at' => $at], [
                'id' => type_integer(),
                'at' => type_datetime(),
            ])]),
        );
    }

    public function test_encode_leaves_null_values_untouched(): void
    {
        static::assertSame(
            [['id' => null, 'name' => null]],
            (new ParquetEncoder(ParquetSchema::with(
                FlatColumn::int64('id'),
                FlatColumn::string('name'),
            )))->encode([new TypedRowValues(['id' => null, 'name' => null], [
                'id' => type_integer(),
                'name' => type_string(),
            ])]),
        );
    }

    public function test_encode_renders_uuid_values_back_to_strings(): void
    {
        static::assertSame(
            [['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new ParquetEncoder(ParquetSchema::with(FlatColumn::uuid('id'))))->encode([new TypedRowValues([
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ], ['id' => type_uuid()])]),
        );
    }

    public function test_decode_normalizes_json_and_uuid_nested_in_struct(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('body', [
            FlatColumn::json('data'),
            FlatColumn::uuid('id'),
            FlatColumn::int64('n'),
        ]))))->decode([[
            'body' => ['data' => '{"a":1}', 'id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'n' => 1],
        ]]);

        // @mago-ignore analysis:mixed-assignment
        $body = $decoded[0]->values['body'];

        static::assertIsArray($body);
        static::assertInstanceOf(Json::class, $body['data']);
        static::assertInstanceOf(Uuid::class, $body['id']);
        static::assertSame(1, $body['n']);
    }

    public function test_decode_normalizes_json_in_deep_struct(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('outer', [NestedColumn::struct('inner', [FlatColumn::json(
            'deep',
        )])]))))->decode([['outer' => ['inner' => ['deep' => '{"e":5}']]]]);

        // @mago-ignore analysis:mixed-assignment
        $outer = $decoded[0]->values['outer'];

        static::assertIsArray($outer);
        // @mago-ignore analysis:mixed-assignment
        $inner = $outer['inner'];
        static::assertIsArray($inner);
        static::assertInstanceOf(Json::class, $inner['deep']);
    }

    public function test_decode_normalizes_json_list_elements(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(NestedColumn::list(
            'items',
            ListElement::json(),
        ))))->decode([['items' => ['{"c":3}', '{"d":4}']]]);

        // @mago-ignore analysis:mixed-assignment
        $items = $decoded[0]->values['items'];

        static::assertIsArray($items);
        static::assertContainsOnlyInstancesOf(Json::class, $items);
    }

    public function test_decode_normalizes_nested_null_leaves_untouched(): void
    {
        static::assertSame(
            ['body' => ['data' => null, 'n' => 1]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('body', [
                FlatColumn::json('data'),
                FlatColumn::int64('n'),
            ]))))->decode([['body' => ['data' => null, 'n' => 1]]])[0]->values,
        );
    }

    public function test_decode_normalizes_uuid_map_values_keeping_keys(): void
    {
        $decoded = (new ParquetEncoder(ParquetSchema::with(NestedColumn::map(
            'ids',
            MapKey::string(),
            MapValue::uuid(),
        ))))->decode([['ids' => ['a' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']]]);

        // @mago-ignore analysis:mixed-assignment
        $ids = $decoded[0]->values['ids'];

        static::assertIsArray($ids);
        static::assertSame(['a'], array_keys($ids));
        static::assertInstanceOf(Uuid::class, $ids['a']);
    }

    public function test_encode_stringifies_json_and_uuid_nested_in_struct(): void
    {
        static::assertSame(
            [['body' => ['data' => '{"a":1}', 'id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'n' => 1]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('body', [
                FlatColumn::json('data'),
                FlatColumn::uuid('id'),
                FlatColumn::int64('n'),
            ]))))->encode([new TypedRowValues([
                'body' => [
                    'data' => Json::fromArray(['a' => 1]),
                    'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                    'n' => 1,
                ],
            ], ['body' => type_structure([
                'data' => type_json(),
                'id' => type_uuid(),
                'n' => type_integer(),
            ])])]),
        );
    }

    public function test_encode_stringifies_json_in_deep_struct(): void
    {
        static::assertSame(
            [['outer' => ['inner' => ['deep' => '{"e":5}']]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::struct('outer', [NestedColumn::struct('inner', [FlatColumn::json(
                'deep',
            )])]))))->encode([new TypedRowValues([
                'outer' => ['inner' => ['deep' => Json::fromArray(['e' => 5])]],
            ], ['outer' => type_structure(['inner' => type_structure(['deep' => type_json()])])])]),
        );
    }

    public function test_encode_stringifies_uuid_list_elements(): void
    {
        static::assertSame(
            [['ids' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479']]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::list(
                'ids',
                ListElement::uuid(),
            ))))->encode([new TypedRowValues([
                'ids' => [new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')],
            ], ['ids' => type_list(type_uuid())])]),
        );
    }

    public function test_encode_stringifies_uuid_map_values_keeping_keys(): void
    {
        static::assertSame(
            [['ids' => ['a' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::map(
                'ids',
                MapKey::string(),
                MapValue::uuid(),
            ))))->encode([new TypedRowValues([
                'ids' => ['a' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')],
            ], ['ids' => type_map(type_string(), type_uuid())])]),
        );
    }

    public function test_encode_stringifies_uuids_in_list_of_structs(): void
    {
        static::assertSame(
            [['items' => [['x' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']]]],
            (new ParquetEncoder(ParquetSchema::with(NestedColumn::list('items', ListElement::structure([FlatColumn::uuid(
                'x',
            )])))))->encode([new TypedRowValues([
                'items' => [['x' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]],
            ], ['items' => type_list(type_structure(['x' => type_uuid()]))])]),
        );
    }
}
