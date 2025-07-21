<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_capturing_groups_with_multiple_matches() : void
    {
        $result = ref('str')->stringMatchAll('/(\w+)=(\d+)/')->eval(
            row(str_entry('str', 'width=100 height=200 depth=50'))
        );

        $expected = [
            ['width=100', 'width', '100'],
            ['height=200', 'height', '200'],
            ['depth=50', 'depth', '50'],
        ];

        self::assertEquals($expected, $result);
    }

    public function test_case_insensitive_match() : void
    {
        $result = ref('str')->stringMatchAll('/hello/i')->eval(
            row(str_entry('str', 'Hello world HELLO universe hello again'))
        );

        self::assertEquals([['Hello'], ['HELLO'], ['hello']], $result);
    }

    public function test_complex_regex_patterns() : void
    {
        $result = ref('str')->stringMatchAll('/(\w+)@(\w+\.\w+)/')->eval(
            row(str_entry('str', 'Contact: user@example.com and admin@test.org'))
        );

        $expected = [
            ['user@example.com', 'user', 'example.com'],
            ['admin@test.org', 'admin', 'test.org'],
        ];

        self::assertEquals($expected, $result);
    }

    public function test_empty_haystack_string() : void
    {
        $result = ref('str')->stringMatchAll('/hello/')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals([], $result);
    }

    public function test_global_flag_behavior() : void
    {
        $result = ref('str')->stringMatchAll('/\w+/')->eval(
            row(str_entry('str', 'one two three'))
        );

        self::assertEquals([['one'], ['two'], ['three']], $result);
    }

    public function test_multiline_match() : void
    {
        $result = ref('str')->stringMatchAll('/^line\d+/m')->eval(
            row(str_entry('str', "line1\nline2\nother\nline3"))
        );

        self::assertEquals([['line1'], ['line2'], ['line3']], $result);
    }

    public function test_multiple_successful_pattern_matches() : void
    {
        $result = ref('str')->stringMatchAll('/\d+/')->eval(
            row(str_entry('str', 'test 123 and 456 and 789'))
        );

        self::assertEquals([['123'], ['456'], ['789']], $result);
    }

    public function test_named_capturing_groups_multiple_matches() : void
    {
        $result = ref('str')->stringMatchAll('/(?P<key>\w+)=(?P<value>\d+)/')->eval(
            row(str_entry('str', 'width=100 height=200'))
        );

        self::assertCount(2, $result);

        // First match
        self::assertArrayHasKey('key', $result[0]);
        self::assertArrayHasKey('value', $result[0]);
        self::assertEquals('width', $result[0]['key']);
        self::assertEquals('100', $result[0]['value']);

        // Second match
        self::assertArrayHasKey('key', $result[1]);
        self::assertArrayHasKey('value', $result[1]);
        self::assertEquals('height', $result[1]['key']);
        self::assertEquals('200', $result[1]['value']);
    }

    public function test_no_matches_found() : void
    {
        $result = ref('str')->stringMatchAll('/foo/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals([], $result);
    }

    public function test_null_haystack() : void
    {
        $result = ref('str')->stringMatchAll('/hello/')->eval(
            row(str_entry('str', null))
        );

        self::assertEquals([], $result);
    }

    public function test_null_pattern() : void
    {
        $result = ref('str')->stringMatchAll(ref('pattern'))->eval(
            row(
                str_entry('str', 'hello world'),
                str_entry('pattern', null)
            )
        );

        self::assertEquals([], $result);
    }

    public function test_numeric_patterns() : void
    {
        $result = ref('str')->stringMatchAll('/\d+\.\d+/')->eval(
            row(str_entry('str', 'Price: 19.99 and 5.50 total: 25.49'))
        );

        self::assertEquals([['19.99'], ['5.50'], ['25.49']], $result);
    }

    public function test_overlapping_patterns() : void
    {
        $result = ref('str')->stringMatchAll('/\w\w/')->eval(
            row(str_entry('str', 'abcdef'))
        );

        self::assertEquals([['ab'], ['cd'], ['ef']], $result);
    }

    public function test_pattern_with_anchors() : void
    {
        $result = ref('str')->stringMatchAll('/^test/')->eval(
            row(str_entry('str', 'test line\ntest another\nnot test'))
        );

        self::assertEquals([['test']], $result);
    }

    public function test_single_character_matches() : void
    {
        $result = ref('str')->stringMatchAll('/[aeiou]/')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals([['e'], ['o'], ['o']], $result);
    }

    public function test_unicode_no_matches() : void
    {
        $result = ref('str')->stringMatchAll('/नमस्कार/u')->eval(
            row(str_entry('str', 'hello world'))
        );

        self::assertEquals([], $result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->stringMatchAll('/\S+/u')->eval(
            row(str_entry('str', 'नमस्ते world स्वागत'))
        );

        self::assertEquals([['नमस्ते'], ['world'], ['स्वागत']], $result);
    }

    public function test_whitespace_matches() : void
    {
        $result = ref('str')->stringMatchAll('/\s+/')->eval(
            row(str_entry('str', 'word1   word2'))
        );

        self::assertEquals([['   ']], $result);
    }

    public function test_with_emoji() : void
    {
        $result = ref('str')->stringMatchAll('/\p{Emoji}/u')->eval(
            row(str_entry('str', 'hello🚀world🎉test'))
        );

        self::assertEquals([['🚀'], ['🎉']], $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->stringMatchAll(ref('pattern'))->eval(
            row(
                str_entry('str', 'test 123 and 456'),
                str_entry('pattern', '/\d+/')
            )
        );

        self::assertEquals([['123'], ['456']], $result);
    }

    public function test_with_special_characters() : void
    {
        $result = ref('str')->stringMatchAll('/[@#$]+/')->eval(
            row(str_entry('str', 'hello@#$ world ### test @@@'))
        );

        self::assertEquals([['@#$'], ['###'], ['@@@']], $result);
    }

    public function test_word_boundaries() : void
    {
        $result = ref('str')->stringMatchAll('/\btest\b/')->eval(
            row(str_entry('str', 'test testing tested test'))
        );

        self::assertEquals([['test'], ['test']], $result);
    }
}
