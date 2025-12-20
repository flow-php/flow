<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\UuidConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class UuidConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new UuidConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_non_uuid_throws_exception() : void
    {
        $converter = new UuidConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_null_handling() : void
    {
        $converter = new UuidConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_object_with_to_string_method() : void
    {
        $converter = new UuidConverter();
        $uuidObject = new class implements \Stringable {
            public function __toString() : string
            {
                return '550e8400-e29b-41d4-a716-446655440000';
            }
        };

        $dbValue = $converter->toDatabase($uuidObject);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $dbValue);
    }

    public function test_string_uuid_passthrough() : void
    {
        $converter = new UuidConverter();
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $converter->toDatabase('550e8400-e29b-41d4-a716-446655440000'));
    }

    public function test_supported_types() : void
    {
        $converter = new UuidConverter();
        self::assertContains(PostgreSqlType::UUID, $converter->supportedTypes());
    }

    public function test_uuid_to_database() : void
    {
        $converter = new UuidConverter();
        $uuid = '550e8400-e29b-41d4-a716-446655440000';

        $dbValue = $converter->toDatabase($uuid);
        self::assertNotNull($dbValue);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $dbValue);
    }
}
