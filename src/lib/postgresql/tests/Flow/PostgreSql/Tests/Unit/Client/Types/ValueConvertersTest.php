<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\Converter\BooleanConverter;
use Flow\PostgreSql\Client\Types\Converter\DateTimeConverter;
use Flow\PostgreSql\Client\Types\Converter\IntArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\IntegerConverter;
use Flow\PostgreSql\Client\Types\Converter\JsonConverter;
use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\Converter\TextArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\UuidArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\UuidConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlVersion;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

use function is_string;

final class ValueConvertersTest extends TestCase
{
    public function test_array_type_detection(): void
    {
        $converters = ValueConverters::create();

        static::assertTrue($converters->has(ValueType::INT4_ARRAY));
        static::assertTrue($converters->has(ValueType::TEXT_ARRAY));
        static::assertTrue($converters->has(ValueType::UUID_ARRAY));

        static::assertInstanceOf(IntArrayConverter::class, $converters->forValueType(ValueType::INT4_ARRAY));
        static::assertInstanceOf(TextArrayConverter::class, $converters->forValueType(ValueType::TEXT_ARRAY));
        static::assertInstanceOf(UuidArrayConverter::class, $converters->forValueType(ValueType::UUID_ARRAY));
    }

    public function test_create_returns_default_converters(): void
    {
        $converters = ValueConverters::create();

        static::assertTrue($converters->has(ValueType::TEXT));
        static::assertTrue($converters->has(ValueType::INT4));
        static::assertTrue($converters->has(ValueType::BOOL));
        static::assertTrue($converters->has(ValueType::TIMESTAMP));
        static::assertTrue($converters->has(ValueType::UUID));
        static::assertTrue($converters->has(ValueType::JSON));
    }

    public function test_fallback_to_string_converter(): void
    {
        $converters = ValueConverters::create();

        $converter = $converters->forValueType(ValueType::OID);
        static::assertInstanceOf(StringConverter::class, $converter);
    }

    public function test_for_postgrecolumn_type_returns_correct_converter(): void
    {
        $converters = ValueConverters::create();

        static::assertInstanceOf(StringConverter::class, $converters->forValueType(ValueType::TEXT));
        static::assertInstanceOf(IntegerConverter::class, $converters->forValueType(ValueType::INT4));
        static::assertInstanceOf(BooleanConverter::class, $converters->forValueType(ValueType::BOOL));
        static::assertInstanceOf(DateTimeConverter::class, $converters->forValueType(ValueType::TIMESTAMP));
        static::assertInstanceOf(UuidConverter::class, $converters->forValueType(ValueType::UUID));
        static::assertInstanceOf(JsonConverter::class, $converters->forValueType(ValueType::JSON));
    }

    public function test_register_adds_custom_converter(): void
    {
        $converters = ValueConverters::create();

        $customConverter = new class implements ValueConverter {
            public function supportedTypes(): array
            {
                return [ValueType::XML];
            }

            public function toDatabase(mixed $value): ?string
            {
                return is_string($value) ? $value : null;
            }
        };

        static::assertFalse($converters->has(ValueType::XML));

        $converters->register($customConverter);

        static::assertTrue($converters->has(ValueType::XML));
        static::assertSame($customConverter, $converters->forValueType(ValueType::XML));
    }

    public function test_unregister_removes_converter(): void
    {
        $converters = ValueConverters::create();

        static::assertTrue($converters->has(ValueType::UUID));

        $converters->unregister(ValueType::UUID);

        static::assertFalse($converters->has(ValueType::UUID));
        static::assertInstanceOf(StringConverter::class, $converters->forValueType(ValueType::UUID));
    }

    public function test_version_specific_multirange_support(): void
    {
        $v13Converters = ValueConverters::create(PostgreSqlVersion::V13);
        $v14Converters = ValueConverters::create(PostgreSqlVersion::V14);
        $v17Converters = ValueConverters::create(PostgreSqlVersion::V17);

        static::assertFalse(PostgreSqlVersion::V13->supportsMultirange());
        static::assertTrue(PostgreSqlVersion::V14->supportsMultirange());
        static::assertTrue(PostgreSqlVersion::V17->supportsMultirange());

        static::assertTrue($v13Converters->has(ValueType::TEXT));
        static::assertTrue($v14Converters->has(ValueType::TEXT));
        static::assertTrue($v17Converters->has(ValueType::TEXT));
    }
}
