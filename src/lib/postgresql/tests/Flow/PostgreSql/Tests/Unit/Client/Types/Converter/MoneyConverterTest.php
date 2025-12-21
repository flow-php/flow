<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\MoneyConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MoneyConverterTest extends TestCase
{
    public static function provide_invalid_values() : \Generator
    {
        yield 'integer' => [100];
        yield 'float' => [123.45];
        yield 'array' => [['array']];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'money with symbol' => ['$99.99', '$99.99'];
        yield 'money with comma' => ['$1,234.56', '$1,234.56'];
        yield 'zero amount' => ['$0.00', '$0.00'];
        yield 'negative amount prefix' => ['-$99.99', '-$99.99'];
        yield 'negative amount suffix' => ['$-99.99', '$-99.99'];
        yield 'euro symbol' => ['€99.99', '€99.99'];
        yield 'pound symbol' => ['£99.99', '£99.99'];
        yield 'yen symbol' => ['¥9999', '¥9999'];
        yield 'no symbol' => ['99.99', '99.99'];
        yield 'european format' => ['1.234,56', '1.234,56'];
        yield 'large amount' => ['$1,234,567.89', '$1,234,567.89'];
        yield 'cents only' => ['$0.01', '$0.01'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value) : void
    {
        $converter = new MoneyConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling() : void
    {
        $converter = new MoneyConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new MoneyConverter();
        self::assertContains(PostgreSqlType::MONEY, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected) : void
    {
        $converter = new MoneyConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
