<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\UuidArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class UuidArrayConverterTest extends TestCase
{
    public function test_empty_array() : void
    {
        $converter = new UuidArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new UuidArrayConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['550e8400-e29b-41d4-a716-446655440000', 123, '6ba7b810-9dad-11d1-80b4-00c04fd430c8']);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new UuidArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new UuidArrayConverter();
        $array = ['550e8400-e29b-41d4-a716-446655440000', null, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{550e8400-e29b-41d4-a716-446655440000,NULL,6ba7b810-9dad-11d1-80b4-00c04fd430c8}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new UuidArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new UuidArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::UUID_ARRAY, $types);
        self::assertCount(1, $types);
    }

    public function test_uuid_array_to_database() : void
    {
        $converter = new UuidArrayConverter();
        $array = [
            '550e8400-e29b-41d4-a716-446655440000',
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
        ];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}', $dbValue);
    }
}
