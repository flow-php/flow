<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ParquetEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
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
}
