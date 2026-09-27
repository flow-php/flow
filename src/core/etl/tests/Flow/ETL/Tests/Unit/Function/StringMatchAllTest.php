<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_empty_haystack_string(): void
    {
        $result = ref('str')
            ->stringMatchAll('/hello/')
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals([], $result);
    }

    public function test_multiple_successful_pattern_matches(): void
    {
        $result = ref('str')
            ->stringMatchAll('/\d+/')
            ->eval(array_to_row(['str' => 'test 123 and 456 and 789'], schema(str_schema('str'))), flow_context());

        static::assertEquals([['123'], ['456'], ['789']], $result);
    }

    public function test_no_matches_found(): void
    {
        $result = ref('str')
            ->stringMatchAll('/foo/')
            ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context());

        static::assertEquals([], $result);
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null haystack');

        ref('str')
            ->stringMatchAll('/hello/')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null pattern');

        ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'pattern' => null],
                    schema(str_schema('str'), str_schema('pattern', nullable: true)),
                ),
                flow_context(),
            );
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->stringMatchAll(ref('pattern'))
            ->eval(
                array_to_row(
                    ['str' => 'test 123 and 456', 'pattern' => '/\d+/'],
                    schema(str_schema('str'), str_schema('pattern')),
                ),
                flow_context(),
            );

        static::assertEquals([['123'], ['456']], $result);
    }
}
