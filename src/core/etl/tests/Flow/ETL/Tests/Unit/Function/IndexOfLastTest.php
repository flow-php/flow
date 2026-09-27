<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic(): void
    {
        static::assertSame(9, ref('str')
            ->indexOfLast('l')
            ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context()));
    }

    public function test_index_of_last_not_found(): void
    {
        static::assertNull(
            ref('str')
                ->indexOfLast('x')
                ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_index_of_last_throws_on_null_needle(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        ref('str')
            ->indexOfLast(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => 'hello', 'needle' => null],
                    schema(str_schema('str'), str_schema('needle', nullable: true)),
                ),
                flow_context(),
            );
    }

    public function test_index_of_last_throws_on_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOfLast function requires non-null string and needle');

        ref('str')
            ->indexOfLast('l')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_index_of_last_with_scalar_function_parameters(): void
    {
        static::assertSame(9, ref('str')
            ->indexOfLast(ref('needle'), ref('ignore_case'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'L', 'ignore_case' => true],
                    schema(str_schema('str'), str_schema('needle'), bool_schema('ignore_case')),
                ),
                flow_context(),
            ));
    }
}
