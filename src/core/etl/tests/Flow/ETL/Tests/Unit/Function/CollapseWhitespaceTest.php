<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_empty_string() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '')),
            flow_context()
        );

        self::assertEquals('', $result);
    }

    public function test_leading_and_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world   ')),
            flow_context()
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_leading_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world')),
            flow_context()
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_mixed_whitespace_types() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\t\tworld\n\ntest")),
            flow_context()
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_multiple_spaces_between_words() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello     world     test')),
            flow_context()
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', null)),
            flow_context()
        );

        self::assertNull($result);
    }

    public function test_single_spaces() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world test')),
            flow_context()
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_single_word() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello')),
            flow_context()
        );

        self::assertEquals('Hello', $result);
    }

    public function test_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world   ')),
            flow_context()
        );

        self::assertEquals('Hello world', $result);
    }
}
