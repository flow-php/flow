<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic(): void
    {
        static::assertSame(9, ref('str')->indexOfLast('l')->eval(row(['str' => 'hello world']), flow_context()));
    }

    public function test_index_of_last_not_found(): void
    {
        static::assertNull(ref('str')->indexOfLast('x')->eval(row(['str' => 'hello world']), flow_context()));
    }

    public function test_index_of_last_throws_on_null_needle(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        ref('str')->indexOfLast(ref('needle'))->eval(row(['str' => 'hello', 'needle' => null]), flow_context());
    }

    public function test_index_of_last_throws_on_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        ref('str')->indexOfLast('l')->eval(row(['str' => null]), flow_context());
    }

    public function test_index_of_last_with_scalar_function_parameters(): void
    {
        static::assertSame(9, ref('str')
            ->indexOfLast(ref('needle'), ref('ignore_case'))
            ->eval(row(['str' => 'hello world', 'needle' => 'L', 'ignore_case' => true]), flow_context()));
    }
}
