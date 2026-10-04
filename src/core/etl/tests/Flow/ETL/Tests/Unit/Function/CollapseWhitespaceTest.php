<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_empty_string(): void
    {
        static::assertEquals('', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_leading_and_trailing_whitespace(): void
    {
        static::assertEquals('Hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => '   Hello world   '],
            schema(str_schema('str')),
        ));
    }

    public function test_leading_whitespace(): void
    {
        static::assertEquals('Hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => '   Hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_mixed_whitespace_types(): void
    {
        static::assertEquals('Hello world test', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => "Hello\t\tworld\n\ntest"],
            schema(str_schema('str')),
        ));
    }

    public function test_multiple_spaces_between_words(): void
    {
        static::assertEquals('Hello world test', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => 'Hello     world     test'],
            schema(str_schema('str')),
        ));
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CollapseWhitespace function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_single_spaces(): void
    {
        static::assertEquals('Hello world test', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => 'Hello world test'],
            schema(str_schema('str')),
        ));
    }

    public function test_single_word(): void
    {
        static::assertEquals('Hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => 'Hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_trailing_whitespace(): void
    {
        static::assertEquals('Hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->collapseWhitespace(),
            ['str' => 'Hello world   '],
            schema(str_schema('str')),
        ));
    }
}
