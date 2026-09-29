<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_empty_haystack_string(): void
    {
        static::assertEquals(
            [],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatchAll('/hello/'),
                ['str' => ''],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_multiple_successful_pattern_matches(): void
    {
        static::assertEquals(
            [['123'], ['456'], ['789']],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatchAll('/\d+/'),
                ['str' => 'test 123 and 456 and 789'],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_no_matches_found(): void
    {
        static::assertEquals(
            [],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatchAll('/foo/'),
                ['str' => 'hello world'],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null haystack');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringMatchAll('/hello/'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatchAll function requires non-null pattern');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringMatchAll(ref('pattern')),
            ['str' => 'hello world', 'pattern' => null],
            schema(str_schema('str'), str_schema('pattern', nullable: true)),
        );
    }

    public function test_with_scalar_function_parameter(): void
    {
        static::assertEquals(
            [['123'], ['456']],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatchAll(ref('pattern')),
                ['str' => 'test 123 and 456', 'pattern' => '/\d+/'],
                schema(str_schema('str'), str_schema('pattern')),
            ),
        );
    }
}
