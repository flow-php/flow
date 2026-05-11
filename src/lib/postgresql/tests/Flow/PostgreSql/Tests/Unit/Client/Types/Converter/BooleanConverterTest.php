<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\BooleanConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BooleanConverterTest extends TestCase
{
    public static function provide_invalid_values(): \Generator
    {
        yield 'integer 1' => [1];
        yield 'integer 0' => [0];
        yield 'float' => [1.0];
        yield 'array' => [[]];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'true' => [true, 't'];
        yield 'false' => [false, 'f'];
        yield 'string t' => ['t', 't'];
        yield 'string f' => ['f', 'f'];
        yield 'string true' => ['true', 'true'];
        yield 'string false' => ['false', 'false'];
        yield 'string 1' => ['1', '1'];
        yield 'string 0' => ['0', '0'];
        yield 'string yes' => ['yes', 'yes'];
        yield 'string no' => ['no', 'no'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new BooleanConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new BooleanConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new BooleanConverter();
        static::assertContains(ValueType::BOOL, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(mixed $input, string $expected): void
    {
        $converter = new BooleanConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
