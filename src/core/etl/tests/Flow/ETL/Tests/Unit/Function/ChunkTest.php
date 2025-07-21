<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class ChunkTest extends FlowTestCase
{
    public function test_chunk_empty_string() : void
    {
        self::assertSame(
            [],
            ref('str')->chunk(3)->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_chunk_equal_to_string_length() : void
    {
        self::assertSame(
            ['hello'],
            ref('str')->chunk(5)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_chunk_larger_than_string_length() : void
    {
        self::assertSame(
            ['hello'],
            ref('str')->chunk(10)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_chunk_negative_size() : void
    {
        self::assertSame(
            [],
            ref('str')->chunk(-1)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_chunk_null_input() : void
    {
        self::assertNull(
            ref('str')->chunk(3)->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_chunk_string_basic_functionality() : void
    {
        self::assertSame(
            ['hel', 'lo '],
            ref('str')->chunk(3)->eval(
                row(str_entry('str', 'hello '))
            )
        );
    }

    public function test_chunk_with_null_size() : void
    {
        self::assertSame(
            [],
            ref('str')->chunk(ref('size'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('size', null)
                )
            )
        );
    }

    public function test_chunk_with_scalar_function_size() : void
    {
        self::assertSame(
            ['he', 'll', 'o'],
            ref('str')->chunk(ref('size'))->eval(
                row(
                    str_entry('str', 'hello'),
                    int_entry('size', 2)
                )
            )
        );
    }

    public function test_chunk_zero_size() : void
    {
        self::assertSame(
            [],
            ref('str')->chunk(0)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }
}
