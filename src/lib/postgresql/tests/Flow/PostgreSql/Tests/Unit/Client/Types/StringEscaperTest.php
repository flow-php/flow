<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\StringEscaper;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StringEscaperTest extends TestCase
{
    public static function provide_escape_always_quoted_cases(): Generator
    {
        yield 'simple string' => ['hello', '"hello"'];
        yield 'empty string' => ['', '""'];
        yield 'json object' => ['{"key":"value"}', '"{\"key\":\"value\"}"'];
        yield 'json array' => ['[1,2,3]', '"[1,2,3]"'];
        yield 'json with nested quotes' => ['{"a":"b","c":"d"}', '"{\"a\":\"b\",\"c\":\"d\"}"'];
        yield 'json with backslash' => ['{"path":"c:\\\\dir"}', '"{\"path\":\"c:\\\\\\\\dir\"}"'];
    }

    public static function provide_escape_cases(): Generator
    {
        yield 'simple string' => ['hello', 'hello'];
        yield 'numeric string' => ['123', '123'];
        yield 'string with underscore' => ['hello_world', 'hello_world'];
        yield 'string with hyphen' => ['hello-world', 'hello-world'];
        yield 'empty string' => ['', '""'];
        yield 'string with space' => ['hello world', '"hello world"'];
        yield 'string with leading space' => [' hello', '" hello"'];
        yield 'string with trailing space' => ['hello ', '"hello "'];
        yield 'string with tab' => ["hello\tworld", "\"hello\tworld\""];
        yield 'string with newline' => ["hello\nworld", "\"hello\nworld\""];
        yield 'string with comma' => ['a,b', '"a,b"'];
        yield 'string with multiple commas' => ['a,b,c,d', '"a,b,c,d"'];
        yield 'string with opening brace' => ['{hello', '"{hello"'];
        yield 'string with closing brace' => ['hello}', '"hello}"'];
        yield 'string with braces' => ['{hello}', '"{hello}"'];
        yield 'string with double quote' => ['"hello"', '"\\"hello\\""'];
        yield 'string with single double quote' => ['say "hi"', '"say \\"hi\\""'];
        yield 'string with backslash' => ['path\\to', '"path\\\\to"'];
        yield 'string with multiple backslashes' => ['c:\\path\\to\\file', '"c:\\\\path\\\\to\\\\file"'];
        yield 'string with backslash and quote' => ['say\\"hi', '"say\\\\\\"hi"'];
        yield 'NULL uppercase' => ['NULL', '"NULL"'];
        yield 'null lowercase' => ['null', '"null"'];
        yield 'Null mixed case' => ['Null', '"Null"'];
        yield 'NULL in sentence' => ['value is NULL', '"value is NULL"'];
        yield 'unicode' => ['日本語', '日本語'];
        yield 'unicode with space' => ['日本語 中文', '"日本語 中文"'];
        yield 'complex escaping' => ['{"key": "value"}', '"{\\"key\\": \\"value\\"}"'];
    }

    #[DataProvider('provide_escape_cases')]
    public function test_escape(string $input, string $expected): void
    {
        static::assertSame($expected, StringEscaper::escape($input));
    }

    #[DataProvider('provide_escape_always_quoted_cases')]
    public function test_escape_always_quoted(string $input, string $expected): void
    {
        static::assertSame($expected, StringEscaper::escapeAlwaysQuoted($input));
    }
}
