<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\MultirangeConverter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MultirangeConverterTest extends TestCase
{
    public static function provide_non_string_values(): \Generator
    {
        yield 'integer' => [12345, ''];
        yield 'array' => [['array'], ''];
        yield 'float' => [3.14, ''];
        yield 'boolean true' => [true, ''];
        yield 'boolean false' => [false, ''];
        yield 'object' => [new \stdClass(), ''];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'multirange' => ['{[1,5),[10,20)}', '{[1,5),[10,20)}'];
        yield 'empty multirange' => ['{}', '{}'];
        yield 'single range' => ['{[1,5)}', '{[1,5)}'];
        yield 'multiple ranges' => ['{[1,5),[10,20),[30,40)}', '{[1,5),[10,20),[30,40)}'];
        yield 'inclusive bounds' => ['{[1,5]}', '{[1,5]}'];
        yield 'exclusive lower' => ['{(1,5]}', '{(1,5]}'];
        yield 'unbounded upper' => ['{[1,)}', '{[1,)}'];
        yield 'unbounded lower' => ['{(,5]}', '{(,5]}'];
        yield 'fully unbounded' => ['{(,)}', '{(,)}'];
        yield 'timestamp multirange' => [
            '{["2024-01-01","2024-06-01"),["2024-07-01","2024-12-31")}',
            '{["2024-01-01","2024-06-01"),["2024-07-01","2024-12-31")}',
        ];
    }

    #[DataProvider('provide_non_string_values')]
    public function test_non_string_returns_empty(mixed $input, string $expected): void
    {
        $converter = new MultirangeConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling(): void
    {
        $converter = new MultirangeConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_empty(): void
    {
        $converter = new MultirangeConverter();
        static::assertSame([], $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected): void
    {
        $converter = new MultirangeConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
