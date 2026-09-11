<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length_ascii_string(): void
    {
        static::assertSame(5, ref('str')->binaryLength()->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_binary_length_binary_data(): void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";

        static::assertSame(5, ref('str')->binaryLength()->eval(row(['str' => $binaryData]), flow_context()));
    }

    public function test_binary_length_empty_string(): void
    {
        static::assertSame(0, ref('str')->binaryLength()->eval(row(['str' => '']), flow_context()));
    }

    public function test_binary_length_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('BinaryLength function requires non-null value');

        ref('str')->binaryLength()->eval(row(['str' => null]), flow_context());
    }

    public function test_binary_length_string_with_newlines_and_tabs(): void
    {
        static::assertSame(12, ref('str')->binaryLength()->eval(row(['str' => "hello\nworld\t"]), flow_context()));
    }
}
