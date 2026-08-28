<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length_ascii_string(): void
    {
        static::assertSame(5, ref('str')->binaryLength()->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_binary_length_binary_data(): void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";

        static::assertSame(5, ref('str')->binaryLength()->eval(row(str_entry('str', $binaryData)), flow_context()));
    }

    public function test_binary_length_empty_string(): void
    {
        static::assertSame(0, ref('str')->binaryLength()->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_binary_length_returns_null_for_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('BinaryLength function requires non-null value');

        ref('str')->binaryLength()->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_binary_length_string_with_newlines_and_tabs(): void
    {
        static::assertSame(12, ref('str')
            ->binaryLength()
            ->eval(row(str_entry('str', "hello\nworld\t")), flow_context()));
    }
}
