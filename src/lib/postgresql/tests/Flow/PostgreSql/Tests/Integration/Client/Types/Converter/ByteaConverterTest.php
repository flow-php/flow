<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_bytea;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;

final class ByteaConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_bytea_values(): \Generator
    {
        yield 'simple string' => ['hello world', 'hello world'];
        yield 'empty string' => ['', ''];
        yield 'special chars' => ['test@#$%^&*()_+=[]{}|;:\'",.<>?/', 'test@#$%^&*()_+=[]{}|;:\'",.<>?/'];
        yield 'unicode' => ['Hello 世界 مرحبا', 'Hello 世界 مرحبا'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_hex_bytea_values(): \Generator
    {
        yield 'simple hex' => ['48454c4c4f', 'HELLO'];
        yield 'empty hex' => ['', ''];
        yield 'mixed case' => ['deadbeef', "\xde\xad\xbe\xef"];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_typed_bytea_binary_values(): \Generator
    {
        yield 'leading null bytes' => ["\x00\x00\x00\x02\x11\x06value1"];
        yield 'embedded null bytes' => ["hello\x00world"];
        yield 'high bytes' => ["\x01\x02\x03\xff\xfe"];
        yield 'serialize output' => [\serialize(['greeting' => 'world', 'count' => 7])];
    }

    public function test_binary_data_via_hex_literal(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('\\x00010203'), column_type_bytea())->as('val'))->toSql());

        static::assertSame("\x00\x01\x02\x03", $result);
    }

    public function test_binary_data_with_null_bytes_via_hex(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('\\x7465737400646174610a'), column_type_bytea())->as('val'))->toSql());

        static::assertSame("test\x00data\x0a", $result);
    }

    #[DataProvider('provide_hex_bytea_values')]
    public function test_bytea_hex_format(string $hexInput, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('\\x' . $hexInput), column_type_bytea())->as('val'))->toSql());

        static::assertSame($expected, $result);
    }

    #[DataProvider('provide_bytea_values')]
    public function test_bytea_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_bytea())->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    public function test_large_bytea_data(): void
    {
        $data = \str_repeat('abcdefghijklmnopqrstuvwxyz0123456789', 100);

        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_bytea())->as('val'))->toSql(), [$data]);

        static::assertSame($data, $result);
    }

    public function test_null_bytea(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal(null), column_type_bytea())->as('val'))->toSql());

        static::assertNull($result);
    }

    #[DataProvider('provide_typed_bytea_binary_values')]
    public function test_typed_bytea_round_trip_preserves_binary(string $input): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_bytea())->as('val'))->toSql(), [typed(
                $input,
                ValueType::BYTEA,
            )]);

        static::assertSame($input, $result);
    }
}
