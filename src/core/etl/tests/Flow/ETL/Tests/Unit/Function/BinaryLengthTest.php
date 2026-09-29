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

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length_ascii_string(): void
    {
        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->binaryLength(),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_binary_length_binary_data(): void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";

        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->binaryLength(),
            ['str' => $binaryData],
            schema(str_schema('str')),
        ));
    }

    public function test_binary_length_empty_string(): void
    {
        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            ref('str')->binaryLength(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_binary_length_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('BinaryLength function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->binaryLength(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_binary_length_string_with_newlines_and_tabs(): void
    {
        static::assertSame(12, (new FunctionContext(flow_context()))->eval(
            ref('str')->binaryLength(),
            ['str' => "hello\nworld\t"],
            schema(str_schema('str')),
        ));
    }
}
