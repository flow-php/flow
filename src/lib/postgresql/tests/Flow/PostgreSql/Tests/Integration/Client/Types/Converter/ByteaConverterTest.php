<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_bytea, literal, param, select};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ByteaConverterTest extends PostgreSqlTestCase
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
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal('\\x00010203'), column_type_bytea())->as('val'))->toSql());

        self::assertSame("\x00\x01\x02\x03", $result);
    }

    public function test_binary_data_with_null_bytes_via_hex() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal('\\x7465737400646174610a'), column_type_bytea())->as('val'))->toSql());

        self::assertSame("test\x00data\x0a", $result);
    }

    #[DataProvider('provide_hex_bytea_values')]
    public function test_bytea_hex_format(string $hexInput, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal('\\x' . $hexInput), column_type_bytea())->as('val'))->toSql());

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_bytea_values')]
    public function test_bytea_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_bytea())->as('val'))->toSql(), [$input]);

        self::assertSame($expected, $result);
    }

    public function test_large_bytea_data() : void
    {
        $data = \str_repeat('abcdefghijklmnopqrstuvwxyz0123456789', 100);

        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_bytea())->as('val'))->toSql(), [$data]);

        self::assertSame($data, $result);
    }

    public function test_null_bytea() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_bytea())->as('val'))->toSql());

        self::assertNull($result);
    }
}
