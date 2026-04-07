<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\Converter\{BooleanConverter, DateTimeConverter, IntArrayConverter, IntegerConverter, JsonConverter, StringConverter, TextArrayConverter, UuidArrayConverter, UuidConverter};
use Flow\PostgreSql\Client\Types\{PostgreSqlVersion, ValueConverter, ValueConverters, ValueType};
use PHPUnit\Framework\TestCase;

final class ValueConvertersTest extends TestCase
{
    public function test_array_type_detection() : void
    {
        $converters = ValueConverters::create();

        self::assertTrue($converters->has(ValueType::INT4_ARRAY));
        self::assertTrue($converters->has(ValueType::TEXT_ARRAY));
        self::assertTrue($converters->has(ValueType::UUID_ARRAY));

        self::assertInstanceOf(IntArrayConverter::class, $converters->forValueType(ValueType::INT4_ARRAY));
        self::assertInstanceOf(TextArrayConverter::class, $converters->forValueType(ValueType::TEXT_ARRAY));
        self::assertInstanceOf(UuidArrayConverter::class, $converters->forValueType(ValueType::UUID_ARRAY));
    }

    public function test_create_returns_default_converters() : void
    {
        $converters = ValueConverters::create();

        self::assertTrue($converters->has(ValueType::TEXT));
        self::assertTrue($converters->has(ValueType::INT4));
        self::assertTrue($converters->has(ValueType::BOOL));
        self::assertTrue($converters->has(ValueType::TIMESTAMP));
        self::assertTrue($converters->has(ValueType::UUID));
        self::assertTrue($converters->has(ValueType::JSON));
    }

    public function test_fallback_to_string_converter() : void
    {
        $converters = ValueConverters::create();

        $converter = $converters->forValueType(ValueType::OID);
        self::assertInstanceOf(StringConverter::class, $converter);
    }

    public function test_for_postgrecolumn_type_returns_correct_converter() : void
    {
        $converters = ValueConverters::create();

        self::assertInstanceOf(StringConverter::class, $converters->forValueType(ValueType::TEXT));
        self::assertInstanceOf(IntegerConverter::class, $converters->forValueType(ValueType::INT4));
        self::assertInstanceOf(BooleanConverter::class, $converters->forValueType(ValueType::BOOL));
        self::assertInstanceOf(DateTimeConverter::class, $converters->forValueType(ValueType::TIMESTAMP));
        self::assertInstanceOf(UuidConverter::class, $converters->forValueType(ValueType::UUID));
        self::assertInstanceOf(JsonConverter::class, $converters->forValueType(ValueType::JSON));
    }

    public function test_register_adds_custom_converter() : void
    {
        $converters = ValueConverters::create();

        $customConverter = new class implements ValueConverter {
            public function supportedTypes() : array
            {
                return [ValueType::XML];
            }

            public function toDatabase(mixed $value) : ?string
            {
                return \is_string($value) ? $value : null;
            }
        };

        self::assertFalse($converters->has(ValueType::XML));

        $converters->register($customConverter);

        self::assertTrue($converters->has(ValueType::XML));
        self::assertSame($customConverter, $converters->forValueType(ValueType::XML));
    }

    public function test_unregister_removes_converter() : void
    {
        $converters = ValueConverters::create();

        self::assertTrue($converters->has(ValueType::UUID));

        $converters->unregister(ValueType::UUID);

        self::assertFalse($converters->has(ValueType::UUID));
        self::assertInstanceOf(StringConverter::class, $converters->forValueType(ValueType::UUID));
    }

    public function test_version_specific_multirange_support() : void
    {
        $v13Converters = ValueConverters::create(PostgreSqlVersion::V13);
        $v14Converters = ValueConverters::create(PostgreSqlVersion::V14);
        $v17Converters = ValueConverters::create(PostgreSqlVersion::V17);

        self::assertFalse(PostgreSqlVersion::V13->supportsMultirange());
        self::assertTrue(PostgreSqlVersion::V14->supportsMultirange());
        self::assertTrue(PostgreSqlVersion::V17->supportsMultirange());

        self::assertTrue($v13Converters->has(ValueType::TEXT));
        self::assertTrue($v14Converters->has(ValueType::TEXT));
        self::assertTrue($v17Converters->has(ValueType::TEXT));
    }
}
