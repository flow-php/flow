<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_empty_string() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_leading_and_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world   '))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_leading_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_mixed_whitespace_types() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\t\tworld\n\ntest"))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_multiple_spaces_between_words() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello     world     test'))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_single_spaces() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world test'))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_single_word() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello', $result);
    }

    public function test_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world   '))
        );

        self::assertEquals('Hello world', $result);
    }
}
