<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\UuidConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidConverterTest extends TestCase
{
    public function test_non_uuid_returns_empty() : void
    {
        $converter = new UuidConverter();
        self::assertSame('', $converter->toDatabase(12345));
        self::assertSame('', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new UuidConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion_with_uuid_object() : void
    {
        $converter = new UuidConverter();
        $uuid = Uuid::fromString('550e8400-e29b-41d4-a716-446655440000');

        $dbValue = $converter->toDatabase($uuid);
        self::assertNotNull($dbValue);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::UUID);
        self::assertInstanceOf(Uuid::class, $phpValue);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $phpValue->toString());
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

    public function test_to_php_returns_uuid_object() : void
    {
        $converter = new UuidConverter();
        $result = $converter->toPhp('550e8400-e29b-41d4-a716-446655440000', PostgreSqlType::UUID);
        self::assertInstanceOf(Uuid::class, $result);
    }
}
