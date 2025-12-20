<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\Converter\{BooleanConverter, DateTimeConverter, IntArrayConverter, IntegerConverter, JsonConverter, StringConverter, TextArrayConverter, UuidArrayConverter, UuidConverter};
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

        self::assertInstanceOf(IntArrayConverter::class, $converters->forPostgreSqlType(PostgreSqlType::INT4_ARRAY));
        self::assertInstanceOf(TextArrayConverter::class, $converters->forPostgreSqlType(PostgreSqlType::TEXT_ARRAY));
        self::assertInstanceOf(UuidArrayConverter::class, $converters->forPostgreSqlType(PostgreSqlType::UUID_ARRAY));
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
            public function supportedTypes() : array
            {
                return [PostgreSqlType::XML];
            }

            public function toDatabase(mixed $value) : ?string
            {
                return \is_string($value) ? $value : null;
            }
        };

        $newConverters = $converters->with($customConverter);

        self::assertTrue($newConverters->hasConverterFor(PostgreSqlType::XML));
        self::assertFalse($converters->hasConverterFor(PostgreSqlType::XML));
    }
}
