<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_binary_length_binary_data() : void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";

        self::assertSame(
            5,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', $binaryData))
            )
        );
    }

    public function test_binary_length_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_binary_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->binaryLength()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_binary_length_string_with_newlines_and_tabs() : void
    {
        self::assertSame(
            12,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', "hello\nworld\t"))
            )
        );
    }
}
