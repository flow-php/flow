<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{json_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class ContainsAnyTest extends FlowTestCase
{
    public function test_contains_any_case_sensitive() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['Hello', 'WORLD'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_empty_haystack() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['hello', 'world'])->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_contains_any_empty_needles_array() : void
    {
        self::assertFalse(
            ref('str')->containsAny([])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_empty_string_needle() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['', 'foo'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_multiple_needles_multiple_found() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['hello', 'world'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_multiple_needles_one_found() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['foo', 'world', 'bar'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_no_needles_found() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['foo', 'bar', 'baz'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_null_needles() : void
    {
        self::assertFalse(
            ref('str')->containsAny(ref('needles'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    json_entry('needles', null)
                )
            )
        );
    }

    public function test_contains_any_null_string() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['hello', 'world'])->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_contains_any_overlapping_needles() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['hell', 'hello'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_partial_matches() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['wor', 'xyz'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_single_needle_found() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['hello'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_unicode_characters() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['स्ते', 'foo'])->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_contains_any_unicode_no_match() : void
    {
        self::assertFalse(
            ref('str')->containsAny(['नमस्कार', 'hello'])->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_contains_any_whitespace_needles() : void
    {
        self::assertTrue(
            ref('str')->containsAny([' ', '\t'])->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_contains_any_with_emoji() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['🚀', 'foo'])->eval(
                row(str_entry('str', 'hello🚀world'))
            )
        );
    }

    public function test_contains_any_with_scalar_function_parameter() : void
    {
        self::assertTrue(
            ref('str')->containsAny(ref('needles'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    json_entry('needles', ['world', 'foo'])
                )
            )
        );
    }

    public function test_contains_any_with_special_characters() : void
    {
        self::assertTrue(
            ref('str')->containsAny(['@#$', 'foo'])->eval(
                row(str_entry('str', 'hello@#$world'))
            )
        );
    }
}
