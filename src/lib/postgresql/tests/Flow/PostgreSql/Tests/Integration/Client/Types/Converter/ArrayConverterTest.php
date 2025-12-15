<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use PHPUnit\Framework\Attributes\DataProvider;

final class ArrayConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{array<bool>, array<bool>}>
     */
    public static function provide_boolean_arrays() : \Generator
    {
        yield 'empty array' => [[], []];
        yield 'true values' => [[true, true], [true, true]];
        yield 'false values' => [[false, false], [false, false]];
        yield 'mixed values' => [[true, false, true], [true, false, true]];
    }

    /**
     * @return \Generator<string, array{array<float>, array<float>}>
     */
    public static function provide_float_arrays() : \Generator
    {
        yield 'empty array' => [[], []];
        yield 'single element' => [[3.14], [3.14]];
        yield 'multiple elements' => [[1.1, 2.2, 3.3], [1.1, 2.2, 3.3]];
    }

    /**
     * @return \Generator<string, array{array<int>, array<int>}>
     */
    public static function provide_integer_arrays() : \Generator
    {
        yield 'empty array' => [[], []];
        yield 'single element' => [[1], [1]];
        yield 'multiple elements' => [[1, 2, 3], [1, 2, 3]];
        yield 'negative numbers' => [[-1, -2, 3], [-1, -2, 3]];
    }

    /**
     * @return \Generator<string, array{array<string>, array<string>}>
     */
    public static function provide_special_char_arrays() : \Generator
    {
        yield 'with quotes' => [['"quoted"'], ['"quoted"']];
        yield 'with backslash' => [['back\\slash'], ['back\\slash']];
        yield 'with comma' => [['a,b', 'c,d'], ['a,b', 'c,d']];
        yield 'with curly braces' => [['{value}'], ['{value}']];
    }

    /**
     * @return \Generator<string, array{array<string>, array<string>}>
     */
    public static function provide_text_arrays() : \Generator
    {
        yield 'empty array' => [[], []];
        yield 'single element' => [['hello'], ['hello']];
        yield 'multiple elements' => [['a', 'b', 'c'], ['a', 'b', 'c']];
        yield 'with spaces' => [['hello world', 'foo bar'], ['hello world', 'foo bar']];
    }

    public function test_array_with_null_elements() : void
    {
        $result = $this->fetchValue('SELECT ARRAY[1, NULL, 3]::int[] AS val');

        self::assertSame([1, null, 3], $result);
    }

    /**
     * @param array<bool> $input
     * @param array<bool> $expected
     */
    #[DataProvider('provide_boolean_arrays')]
    public function test_boolean_array_round_trip(array $input, array $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::bool[] AS val', [$input]);

        self::assertSame($expected, $result);
    }

    /**
     * @param array<float> $input
     * @param array<float> $expected
     */
    #[DataProvider('provide_float_arrays')]
    public function test_float_array_round_trip(array $input, array $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::float8[] AS val', [$input]);

        self::assertEquals($expected, $result);
    }

    /**
     * @param array<int> $input
     * @param array<int> $expected
     */
    #[DataProvider('provide_integer_arrays')]
    public function test_integer_array_round_trip(array $input, array $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::int[] AS val', [$input]);

        self::assertSame($expected, $result);
    }

    public function test_null_array() : void
    {
        $result = $this->fetchValue('SELECT NULL::int[] AS val');

        self::assertNull($result);
    }

    /**
     * @param array<string> $input
     * @param array<string> $expected
     */
    #[DataProvider('provide_text_arrays')]
    public function test_text_array_round_trip(array $input, array $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::text[] AS val', [$input]);

        self::assertSame($expected, $result);
    }

    /**
     * @param array<string> $input
     * @param array<string> $expected
     */
    #[DataProvider('provide_special_char_arrays')]
    public function test_text_array_with_special_chars(array $input, array $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::text[] AS val', [$input]);

        self::assertSame($expected, $result);
    }
}
