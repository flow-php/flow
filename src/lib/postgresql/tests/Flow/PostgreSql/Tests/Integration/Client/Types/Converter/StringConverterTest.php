<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_char;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function str_repeat;

final class StringConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_char_values(): Generator
    {
        yield 'padded' => ['test', 'test      '];
        yield 'exact length' => ['1234567890', '1234567890'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_text_values(): Generator
    {
        yield 'simple string' => ['hello world', 'hello world'];
        yield 'empty string' => ['', ''];
        yield 'unicode' => ['こんにちは', 'こんにちは'];
        yield 'emoji' => ['Hello 👋 World 🌍', 'Hello 👋 World 🌍'];
        yield 'special chars' => ["O'Reilly", "O'Reilly"];
        yield 'backslash' => ['path\\to\\file', 'path\\to\\file'];
        yield 'newlines' => ["line1\nline2\nline3", "line1\nline2\nline3"];
        yield 'tabs' => ["col1\tcol2\tcol3", "col1\tcol2\tcol3"];
        yield 'quotes' => ['"quoted"', '"quoted"'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_varchar_values(): Generator
    {
        yield 'simple' => ['test', 'test'];
        yield 'max length' => [str_repeat('a', 255), str_repeat('a', 255)];
    }

    #[DataProvider('provide_char_values')]
    public function test_char_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_char(10))->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    public function test_null_text(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_text())->as('val'))->toSql()),
        );
    }

    #[DataProvider('provide_text_values')]
    public function test_text_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_text())->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    #[DataProvider('provide_varchar_values')]
    public function test_varchar_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_varchar(255))->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }
}
