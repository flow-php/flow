<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use PHPUnit\Framework\Attributes\DataProvider;

final class BooleanConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{string, bool}>
     */
    public static function provide_boolean_strings() : \Generator
    {
        yield 'string true' => ['true', true];
        yield 'string false' => ['false', false];
        yield 'string t' => ['t', true];
        yield 'string f' => ['f', false];
        yield 'string yes' => ['yes', true];
        yield 'string no' => ['no', false];
        yield 'string 1' => ['1', true];
        yield 'string 0' => ['0', false];
    }

    /**
     * @return \Generator<string, array{bool, bool}>
     */
    public static function provide_boolean_values() : \Generator
    {
        yield 'true' => [true, true];
        yield 'false' => [false, false];
    }

    #[DataProvider('provide_boolean_values')]
    public function test_boolean_round_trip(bool $input, bool $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::bool AS val', [$input]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_boolean_strings')]
    public function test_boolean_string_conversion(string $input, bool $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::bool AS val', [$input]);

        self::assertSame($expected, $result);
    }

    public function test_null_boolean() : void
    {
        $result = $this->fetchValue('SELECT NULL::bool AS val');

        self::assertNull($result);
    }
}
