<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class ChunkTest extends FlowTestCase
{
    public function test_chunk_empty_string(): void
    {
        static::assertSame([], ref('str')->chunk(3)->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_chunk_equal_to_string_length(): void
    {
        static::assertSame(['hello'], ref('str')->chunk(5)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_chunk_larger_than_string_length(): void
    {
        static::assertSame(['hello'], ref('str')->chunk(10)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_chunk_negative_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')->chunk(-1)->eval(row(str_entry('str', 'hello')), flow_context());
    }

    public function test_chunk_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null value');

        ref('str')->chunk(3)->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_chunk_string_basic_functionality(): void
    {
        static::assertSame(['hel', 'lo '], ref('str')->chunk(3)->eval(row(str_entry('str', 'hello ')), flow_context()));
    }

    public function test_chunk_with_null_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')->chunk(ref('size'))->eval(row(str_entry('str', 'hello'), str_entry('size', null)), flow_context());
    }

    public function test_chunk_with_scalar_function_size(): void
    {
        static::assertSame(
            ['he', 'll', 'o'],
            ref('str')->chunk(ref('size'))->eval(row(str_entry('str', 'hello'), int_entry('size', 2)), flow_context()),
        );
    }

    public function test_chunk_zero_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')->chunk(0)->eval(row(str_entry('str', 'hello')), flow_context());
    }
}
