<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic(): void
    {
        static::assertSame(9, (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOfLast('l'),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_index_of_last_not_found(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->indexOfLast('x'),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_index_of_last_throws_on_null_needle(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOfLast(ref('needle')),
            ['str' => 'hello', 'needle' => null],
            schema(str_schema('str'), str_schema('needle', nullable: true)),
        );
    }

    public function test_index_of_last_throws_on_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOfLast('l'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_index_of_last_with_scalar_function_parameters(): void
    {
        static::assertSame(9, (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOfLast(ref('needle'), ref('ignore_case')),
            ['str' => 'hello world', 'needle' => 'L', 'ignore_case' => true],
            schema(str_schema('str'), str_schema('needle'), bool_schema('ignore_case')),
        ));
    }
}
