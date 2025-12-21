<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\UuidArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class UuidArrayConverterTest extends TestCase
{
    public static function provide_non_array_values() : \Generator
    {
        yield 'string' => ['not an array', '{}'];
        yield 'integer' => [12345, '{}'];
        yield 'float' => [3.14, '{}'];
        yield 'boolean true' => [true, '{}'];
        yield 'boolean false' => [false, '{}'];
        yield 'object' => [new \stdClass(), '{}'];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'UUID array' => [['550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8'], '{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}'];
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [['550e8400-e29b-41d4-a716-446655440000', null, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'], '{550e8400-e29b-41d4-a716-446655440000,NULL,6ba7b810-9dad-11d1-80b4-00c04fd430c8}'];
        yield 'single UUID' => [['550e8400-e29b-41d4-a716-446655440000'], '{550e8400-e29b-41d4-a716-446655440000}'];
        yield 'uppercase UUIDs' => [['550E8400-E29B-41D4-A716-446655440000', '6BA7B810-9DAD-11D1-80B4-00C04FD430C8'], '{550E8400-E29B-41D4-A716-446655440000,6BA7B810-9DAD-11D1-80B4-00C04FD430C8}'];
        yield 'nil UUIDs' => [['00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'], '{00000000-0000-0000-0000-000000000000,00000000-0000-0000-0000-000000000000}'];
        yield 'mixed case UUIDs' => [['550E8400-e29b-41D4-a716-446655440000'], '{550E8400-e29b-41D4-a716-446655440000}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'max UUIDs' => [['ffffffff-ffff-ffff-ffff-ffffffffffff'], '{ffffffff-ffff-ffff-ffff-ffffffffffff}'];
    }

    public function test_invalid_element_throws_exception() : void
    {
        $converter = new UuidArrayConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['550e8400-e29b-41d4-a716-446655440000', 123, '6ba7b810-9dad-11d1-80b4-00c04fd430c8']);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected) : void
    {
        $converter = new UuidArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
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

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected) : void
    {
        $converter = new UuidArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
