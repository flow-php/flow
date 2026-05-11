<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\UuidConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class UuidConverterTest extends TestCase
{
    public static function provide_invalid_values(): \Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'UUID string lowercase' => [
            '550e8400-e29b-41d4-a716-446655440000',
            '550e8400-e29b-41d4-a716-446655440000',
        ];
        yield 'UUID string uppercase' => [
            '550E8400-E29B-41D4-A716-446655440000',
            '550E8400-E29B-41D4-A716-446655440000',
        ];
        yield 'UUID string mixed case' => [
            '550e8400-E29B-41d4-A716-446655440000',
            '550e8400-E29B-41d4-A716-446655440000',
        ];
        yield 'nil UUID' => ['00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'];
        yield 'UUID v1' => ['6ba7b810-9dad-11d1-80b4-00c04fd430c8', '6ba7b810-9dad-11d1-80b4-00c04fd430c8'];
        yield 'UUID v4' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'];
        yield 'max UUID' => ['ffffffff-ffff-ffff-ffff-ffffffffffff', 'ffffffff-ffff-ffff-ffff-ffffffffffff'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new UuidConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new UuidConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_stringable_object(): void
    {
        $converter = new UuidConverter();
        $uuidObject = new class implements \Stringable {
            public function __toString(): string
            {
                return '550e8400-e29b-41d4-a716-446655440000';
            }
        };

        $dbValue = $converter->toDatabase($uuidObject);
        static::assertSame('550e8400-e29b-41d4-a716-446655440000', $dbValue);
    }

    public function test_supported_types(): void
    {
        $converter = new UuidConverter();
        static::assertContains(ValueType::UUID, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected): void
    {
        $converter = new UuidConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
