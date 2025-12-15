<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_json, type_string, type_uuid};
use Flow\PostgreSql\Client\Types\Converter\{ArrayConverter, BooleanConverter, DateTimeConverter, FloatConverter, IntegerConverter, JsonConverter, StringConverter, UuidConverter};
use Flow\PostgreSql\Client\Types\{PostgreSqlType, PostgreSqlVersion, ValueConverter, ValueConverters};
use PHPUnit\Framework\TestCase;

final class ValueConvertersTest extends TestCase
{
    public function test_array_type_detection() : void
    {
        $converters = ValueConverters::create();

        self::assertTrue($converters->hasConverterFor(PostgreSqlType::INT4_ARRAY));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::TEXT_ARRAY));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::UUID_ARRAY));

        self::assertInstanceOf(ArrayConverter::class, $converters->forPostgreSqlType(PostgreSqlType::INT4_ARRAY));
        self::assertInstanceOf(ArrayConverter::class, $converters->forPostgreSqlType(PostgreSqlType::TEXT_ARRAY));
    }

    public function test_create_returns_default_converters() : void
    {
        $converters = ValueConverters::create();

        self::assertTrue($converters->hasConverterFor(PostgreSqlType::TEXT));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::INT4));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::BOOL));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::TIMESTAMP));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::UUID));
        self::assertTrue($converters->hasConverterFor(PostgreSqlType::JSON));
    }

    public function test_fallback_to_string_converter() : void
    {
        $converters = ValueConverters::create();

        $converter = $converters->forPostgreSqlType(PostgreSqlType::OID);
        self::assertInstanceOf(StringConverter::class, $converter);
    }

    public function test_for_flow_type_returns_correct_converter() : void
    {
        $converters = ValueConverters::create();

        self::assertInstanceOf(IntegerConverter::class, $converters->forFlowType(type_integer()));
        self::assertInstanceOf(FloatConverter::class, $converters->forFlowType(type_float()));
        self::assertInstanceOf(BooleanConverter::class, $converters->forFlowType(type_boolean()));
        self::assertInstanceOf(UuidConverter::class, $converters->forFlowType(type_uuid()));
        self::assertInstanceOf(JsonConverter::class, $converters->forFlowType(type_json()));
    }

    public function test_for_postgredata_type_returns_correct_converter() : void
    {
        $converters = ValueConverters::create();

        self::assertInstanceOf(StringConverter::class, $converters->forPostgreSqlType(PostgreSqlType::TEXT));
        self::assertInstanceOf(IntegerConverter::class, $converters->forPostgreSqlType(PostgreSqlType::INT4));
        self::assertInstanceOf(BooleanConverter::class, $converters->forPostgreSqlType(PostgreSqlType::BOOL));
        self::assertInstanceOf(DateTimeConverter::class, $converters->forPostgreSqlType(PostgreSqlType::TIMESTAMP));
        self::assertInstanceOf(UuidConverter::class, $converters->forPostgreSqlType(PostgreSqlType::UUID));
        self::assertInstanceOf(JsonConverter::class, $converters->forPostgreSqlType(PostgreSqlType::JSON));
    }

    public function test_version_specific_multirange_support() : void
    {
        $v13Converters = ValueConverters::create(PostgreSqlVersion::V13);
        $v14Converters = ValueConverters::create(PostgreSqlVersion::V14);
        $v17Converters = ValueConverters::create(PostgreSqlVersion::V17);

        self::assertFalse(PostgreSqlVersion::V13->supportsMultirange());
        self::assertTrue(PostgreSqlVersion::V14->supportsMultirange());
        self::assertTrue(PostgreSqlVersion::V17->supportsMultirange());

        self::assertTrue($v13Converters->hasConverterFor(PostgreSqlType::TEXT));
        self::assertTrue($v14Converters->hasConverterFor(PostgreSqlType::TEXT));
        self::assertTrue($v17Converters->hasConverterFor(PostgreSqlType::TEXT));
    }

    public function test_with_adds_custom_converter() : void
    {
        $converters = ValueConverters::create();

        $customConverter = new class implements ValueConverter {
            public function flowType() : \Flow\Types\Type
            {
                return type_string();
            }

            public function supportedTypes() : array
            {
                return [PostgreSqlType::XML];
            }

            public function toDatabase(mixed $value) : ?string
            {
                return \is_string($value) ? $value : null;
            }

            public function toPhp(string $value, PostgreSqlType $type) : string
            {
                return $value;
            }
        };

        $newConverters = $converters->with($customConverter);

        self::assertTrue($newConverters->hasConverterFor(PostgreSqlType::XML));
        self::assertFalse($converters->hasConverterFor(PostgreSqlType::XML));
    }
}
