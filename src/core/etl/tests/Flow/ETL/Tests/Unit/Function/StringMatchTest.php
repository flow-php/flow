<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchTest extends FlowTestCase
{
    public function test_capturing_groups() : void
    {
        $result = ref('str')->stringMatch('/(\w+)\s+(\w+)/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['hello world', 'hello', 'world'], $result);
    }

    public function test_case_insensitive_match() : void
    {
        $result = ref('str')->stringMatch('/HELLO/i')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['hello'], $result);
    }

    public function test_complex_regex_patterns() : void
    {
        $result = ref('str')->stringMatch('/(\w+)@(\w+\.\w+)/')->eval(
            row(str_entry('str', 'user@example.com'))
        );

        self::assertEquals(['user@example.com', 'user', 'example.com'], $result);
    }

    public function test_empty_haystack_string() : void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(
            row(str_entry('str', ''))
        );

        self::assertNull($result);
    }

    public function test_invalid_regex_pattern() : void
    {
        $result = ref('str')->stringMatch('/[/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertNull($result);
    }

    public function test_multiline_match() : void
    {
        $result = ref('str')->stringMatch('/^hello/m')->eval(
            row(str_entry('str', "line1\nhello world"))
        );

        self::assertEquals(['hello'], $result);
    }

    public function test_named_capturing_groups() : void
    {
        $result = ref('str')->stringMatch('/(?P<first>\w+)\s+(?P<second>\w+)/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertIsArray($result);
        self::assertArrayHasKey('first', $result);
        self::assertArrayHasKey('second', $result);
        self::assertEquals('hello', $result['first']);
        self::assertEquals('world', $result['second']);
    }

    public function test_no_matches_found() : void
    {
        $result = ref('str')->stringMatch('/foo/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertNull($result);
    }

    public function test_null_haystack() : void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_null_pattern() : void
    {
        $result = ref('str')->stringMatch(ref('pattern'))->eval(
            row(
                str_entry('str', 'hello world'),
                str_entry('pattern', null)
            )
        );

        self::assertNull($result);
    }

    public function test_partial_matches() : void
    {
        $result = ref('str')->stringMatch('/wor/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['wor'], $result);
    }

    public function test_pattern_with_anchors() : void
    {
        $result = ref('str')->stringMatch('/^hello/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['hello'], $result);
    }

    public function test_pattern_with_end_anchor_match() : void
    {
        $result = ref('str')->stringMatch('/world$/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['world'], $result);
    }

    public function test_pattern_with_end_anchor_no_match() : void
    {
        $result = ref('str')->stringMatch('/hello$/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertNull($result);
    }

    public function test_pattern_with_whitespace() : void
    {
        $result = ref('str')->stringMatch('/\s+/')->eval(
            row(str_entry('str', 'hello   world'))
        );

        self::assertEquals(['   '], $result);
    }

    public function test_successful_pattern_match() : void
    {
        $result = ref('str')->stringMatch('/hello/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals(['hello'], $result);
    }

    public function test_unicode_no_match() : void
    {
        $result = ref('str')->stringMatch('/नमस्कार/u')->eval(
            row(str_entry('str', 'नमस्ते world'))
        );

        self::assertNull($result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->stringMatch('/नमस्ते/u')->eval(
            row(str_entry('str', 'नमस्ते world'))
        );

        self::assertEquals(['नमस्ते'], $result);
    }

    public function test_with_emoji() : void
    {
        $result = ref('str')->stringMatch('/🚀/u')->eval(
            row(str_entry('str', 'hello🚀world'))
        );

        self::assertEquals(['🚀'], $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->stringMatch(ref('pattern'))->eval(
            row(
                str_entry('str', 'hello world'),
                str_entry('pattern', '/world/')
            )
        );

        self::assertEquals(['world'], $result);
    }

    public function test_with_special_characters() : void
    {
        $result = ref('str')->stringMatch('/[@#$]+/')->eval(
            row(str_entry('str', 'hello@#$world'))
        );

        self::assertEquals(['@#$'], $result);
    }
}
