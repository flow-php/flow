<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use PHPUnit\Framework\Attributes\DataProvider;

final class ByteaConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_bytea_values() : \Generator
    {
        yield 'simple string' => ['hello world', 'hello world'];
        yield 'empty string' => ['', ''];
        yield 'special chars' => ['test@#$%^&*()_+=[]{}|;:\'",.<>?/', 'test@#$%^&*()_+=[]{}|;:\'",.<>?/'];
        yield 'unicode' => ['Hello 世界 مرحبا', 'Hello 世界 مرحبا'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_hex_bytea_values() : \Generator
    {
        yield 'simple hex' => ['48454c4c4f', 'HELLO'];
        yield 'empty hex' => ['', ''];
        yield 'mixed case' => ['deadbeef', "\xde\xad\xbe\xef"];
    }

    public function test_binary_data_via_hex_literal() : void
    {
        $result = $this->fetchValue("SELECT '\\x00010203'::bytea AS val");

        self::assertSame("\x00\x01\x02\x03", $result);
    }

    public function test_binary_data_with_null_bytes_via_hex() : void
    {
        $result = $this->fetchValue("SELECT '\\x7465737400646174610a'::bytea AS val");

        self::assertSame("test\x00data\x0a", $result);
    }

    #[DataProvider('provide_hex_bytea_values')]
    public function test_bytea_hex_format(string $hexInput, string $expected) : void
    {
        $result = $this->fetchValue("SELECT '\\x{$hexInput}'::bytea AS val");

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_bytea_values')]
    public function test_bytea_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::bytea AS val', [$input]);

        self::assertSame($expected, $result);
    }

    public function test_large_bytea_data() : void
    {
        $data = \str_repeat('abcdefghijklmnopqrstuvwxyz0123456789', 100);

        $result = $this->fetchValue('SELECT $1::bytea AS val', [$data]);

        self::assertSame($data, $result);
    }

    public function test_null_bytea() : void
    {
        $result = $this->fetchValue('SELECT NULL::bytea AS val');

        self::assertNull($result);
    }
}
