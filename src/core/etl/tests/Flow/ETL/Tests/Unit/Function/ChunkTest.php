<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
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
            ref('str')->chunk(3)->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_chunk_equal_to_string_length(): void
    {
        static::assertSame(
            ['hello'],
            ref('str')->chunk(5)->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_chunk_larger_than_string_length(): void
    {
        static::assertSame(
            ['hello'],
            ref('str')->chunk(10)->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_chunk_negative_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')->chunk(-1)->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());
    }

    public function test_chunk_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null value');

        ref('str')
            ->chunk(3)
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_chunk_string_basic_functionality(): void
    {
        static::assertSame(
            ['hel', 'lo '],
            ref('str')->chunk(3)->eval(array_to_row(['str' => 'hello '], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_chunk_with_null_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')
            ->chunk(ref('size'))
            ->eval(
                array_to_row(
                    ['str' => 'hello', 'size' => null],
                    schema(str_schema('str'), str_schema('size', nullable: true)),
                ),
                flow_context(),
            );
    }

    public function test_chunk_with_scalar_function_size(): void
    {
        static::assertSame(
            ['he', 'll', 'o'],
            ref('str')
                ->chunk(ref('size'))
                ->eval(
                    array_to_row(['str' => 'hello', 'size' => 2], schema(str_schema('str'), int_schema('size'))),
                    flow_context(),
                ),
        );
    }

    public function test_chunk_zero_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk function requires non-null, positive size');

        ref('str')->chunk(0)->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context());
    }
}
