<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ChunkTest extends FlowTestCase
{
    public function test_chunk_empty_string(): void
    {
        static::assertSame(
            [],
            (new FunctionContext(flow_context()))->eval(ref('str')->chunk(3), ['str' => ''], schema(str_schema('str'))),
        );
    }

    public function test_chunk_equal_to_string_length(): void
    {
        static::assertSame(
            ['hello'],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->chunk(5),
                ['str' => 'hello'],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_chunk_larger_than_string_length(): void
    {
        static::assertSame(
            ['hello'],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->chunk(10),
                ['str' => 'hello'],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_chunk_negative_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->chunk(-1),
            ['str' => 'hello'],
            schema(str_schema('str')),
        );
    }

    public function test_chunk_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->chunk(3),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_chunk_string_basic_functionality(): void
    {
        static::assertSame(
            ['hel', 'lo '],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->chunk(3),
                ['str' => 'hello '],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_chunk_with_null_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->chunk(ref('size')),
            ['str' => 'hello', 'size' => null],
            schema(str_schema('str'), str_schema('size', nullable: true)),
        );
    }

    public function test_chunk_with_scalar_function_size(): void
    {
        static::assertSame(
            ['he', 'll', 'o'],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->chunk(ref('size')),
                ['str' => 'hello', 'size' => 2],
                schema(str_schema('str'), int_schema('size')),
            ),
        );
    }

    public function test_chunk_zero_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->chunk(0),
            ['str' => 'hello'],
            schema(str_schema('str')),
        );
    }
}
