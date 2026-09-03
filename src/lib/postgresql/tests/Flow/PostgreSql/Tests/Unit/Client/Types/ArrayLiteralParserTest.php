<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ArrayLiteralParser;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ArrayLiteralParserTest extends TestCase
{
    /**
     * @return Generator<string, array{string, string}>
     */
    public static function provide_malformed_literals(): Generator
    {
        yield 'unterminated' => ['{a', 'it is not terminated'];
        yield 'empty trailing element' => ['{a,}', 'it has an empty trailing element'];
        yield 'empty leading element' => ['{,a}', 'it has an empty element'];
        yield 'empty element between commas' => ['{a,,b}', 'it has an empty element'];
        yield 'no opening brace' => ['abc', 'it does not open with {'];
        yield 'empty literal' => ['', 'it does not open with {'];
        yield 'unterminated quote' => ['{"a', 'it ends inside a quoted element'];
        yield 'two elements without a separator' => ['{"a" "b"}', 'two elements are not separated by a comma'];
        yield 'trailing characters' => ['{a}b', 'it has trailing characters'];
        yield 'nested array without a comma' => ['{a{1}}', 'a nested array must follow a comma'];
        yield 'ends inside an escape' => ['{"a\\', 'it ends inside an escape'];
    }

    /**
     * @return Generator<string, array{string, list<mixed>}>
     */
    public static function provide_well_formed_literals(): Generator
    {
        yield 'empty array' => ['{}', []];
        yield 'plain elements' => ['{a,b}', ['a', 'b']];
        yield 'digits stay strings' => ['{1,2,3}', ['1', '2', '3']];
        yield 'unquoted NULL is the sql null' => ['{NULL,a}', [null, 'a']];
        yield 'quoted NULL is the four character string' => ['{"NULL"}', ['NULL']];
        yield 'commas and braces inside quotes are data' => ['{"a,b","c}d"}', ['a,b', 'c}d']];
        yield 'backslash escapes inside quotes' => ['{"a\\"b","c\\\\d"}', ['a"b', 'c\\d']];
        yield 'nesting' => ['{{1,2},{3}}', [['1', '2'], ['3']]];
        yield 'quoted whitespace is kept, unquoted is trimmed' => ['{" x ",y}', [' x ', 'y']];
        yield 'lowercase null is the sql null' => ['{null}', [null]];
        yield 'whitespace after the closing brace' => ['{a} ', ['a']];
        yield 'an explicit lower bound is skipped' => ['[2:3]={1,2}', ['1', '2']];
    }

    #[DataProvider('provide_malformed_literals')]
    public function test_a_malformed_literal_is_refused(string $literal, string $reason): void
    {
        $this->expectException(ValueConversionException::class);
        $this->expectExceptionMessage($reason);

        (new ArrayLiteralParser())->parse($literal);
    }

    /**
     * @param list<mixed> $expected
     */
    #[DataProvider('provide_well_formed_literals')]
    public function test_a_well_formed_literal_parses(string $literal, array $expected): void
    {
        static::assertSame($expected, (new ArrayLiteralParser())->parse($literal));
    }
}
