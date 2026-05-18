<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\ColumnDefaultFormatter;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;

final class ColumnDefaultFormatterTest extends TestCase
{
    private ColumnDefaultFormatter $formatter;

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->formatter = new ColumnDefaultFormatter();
    }

    public function test_column_reference_expression_is_deparsed(): void
    {
        static::assertSame('other_column', $this->formatter->format(col('other_column')));
    }

    public function test_empty_string_is_empty_literal(): void
    {
        static::assertSame("''", $this->formatter->format(''));
    }

    public function test_false_returns_lowercase_keyword(): void
    {
        static::assertSame('false', $this->formatter->format(false));
    }

    public function test_float_returns_decimal_literal(): void
    {
        static::assertSame('3.14', $this->formatter->format(3.14));
    }

    public function test_function_call_expression_is_deparsed(): void
    {
        static::assertSame('now()', $this->formatter->format(func('now', [])));
    }

    public function test_integer_returns_numeric_literal(): void
    {
        static::assertSame('42', $this->formatter->format(42));
    }

    public function test_literal_expression_is_deparsed(): void
    {
        static::assertSame("'active'", $this->formatter->format(literal('active')));
    }

    public function test_negative_integer_returns_signed_literal(): void
    {
        static::assertSame('-7', $this->formatter->format(-7));
    }

    public function test_null_returns_null(): void
    {
        static::assertNull($this->formatter->format(null));
    }

    public function test_string_is_single_quote_wrapped(): void
    {
        static::assertSame("'pending'", $this->formatter->format('pending'));
    }

    public function test_string_with_embedded_quote_is_doubled(): void
    {
        static::assertSame("'O''Brien'", $this->formatter->format("O'Brien"));
    }

    public function test_true_returns_lowercase_keyword(): void
    {
        static::assertSame('true', $this->formatter->format(true));
    }

    public function test_zero_returns_zero_literal(): void
    {
        static::assertSame('0', $this->formatter->format(0));
    }
}
