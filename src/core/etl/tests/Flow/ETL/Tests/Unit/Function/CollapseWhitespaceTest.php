<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_empty_string(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => '']), flow_context());

        static::assertEquals('', $result);
    }

    public function test_leading_and_trailing_whitespace(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => '   Hello world   ']), flow_context());

        static::assertEquals('Hello world', $result);
    }

    public function test_leading_whitespace(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => '   Hello world']), flow_context());

        static::assertEquals('Hello world', $result);
    }

    public function test_mixed_whitespace_types(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => "Hello\t\tworld\n\ntest"]), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_multiple_spaces_between_words(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => 'Hello     world     test']), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CollapseWhitespace function requires non-null value');

        $result = ref('str')->collapseWhitespace()->eval(row(['str' => null]), flow_context());

        static::assertNull($result);
    }

    public function test_single_spaces(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => 'Hello world test']), flow_context());

        static::assertEquals('Hello world test', $result);
    }

    public function test_single_word(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => 'Hello']), flow_context());

        static::assertEquals('Hello', $result);
    }

    public function test_trailing_whitespace(): void
    {
        $result = ref('str')->collapseWhitespace()->eval(row(['str' => 'Hello world   ']), flow_context());

        static::assertEquals('Hello world', $result);
    }
}
