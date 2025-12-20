<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class NumericConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_numeric_values() : \Generator
    {
        yield 'zero' => ['0', '0'];
        yield 'positive integer' => ['123', '123'];
        yield 'negative integer' => ['-456', '-456'];
        yield 'decimal' => ['123.456', '123.456'];
        yield 'negative decimal' => ['-789.012', '-789.012'];
        yield 'large precision' => ['12345678901234567890.12345678901234567890', '12345678901234567890.12345678901234567890'];
    }

    public function test_null_numeric() : void
    {
        $result = $this->fetchValue('SELECT NULL::numeric AS val');

        self::assertNull($result);
    }

    #[DataProvider('provide_numeric_values')]
    public function test_numeric_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::numeric AS val', [typed($input, PostgreSqlType::NUMERIC)]);

        self::assertSame($expected, $result);
    }

    public function test_numeric_with_precision_and_scale() : void
    {
        $result = $this->fetchValue('SELECT $1::numeric(10,2) AS val', [typed('1234.5678', PostgreSqlType::NUMERIC)]);

        self::assertSame('1234.57', $result);
    }
}
