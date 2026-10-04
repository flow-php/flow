<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringMatchTest extends FlowTestCase
{
    public function test_no_matches_found(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->stringMatch('/foo/'),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null haystack');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->stringMatch('/hello/'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null pattern');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->stringMatch(ref('pattern')),
            ['str' => 'hello world', 'pattern' => null],
            schema(str_schema('str'), str_schema('pattern', nullable: true)),
        ));
    }

    public function test_an_invalid_pattern_keeps_the_regex_error_as_the_cause(): void
    {
        try {
            (new FunctionContext(flow_context()))->eval(
                ref('value')->stringMatch(lit('/(/')),
                ['value' => 'abc'],
                schema(str_schema('value')),
            );
            static::fail('An invalid pattern must be refused.');
        } catch (EvaluationException $e) {
            static::assertStringStartsWith('StringMatch error: ', (string) $e->getPrevious()?->getMessage());
            static::assertNotNull($e->getPrevious()?->getPrevious());
        }
    }

    public function test_successful_pattern_match(): void
    {
        static::assertEquals(
            ['hello'],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatch('/hello/'),
                ['str' => 'hello world'],
                schema(str_schema('str')),
            ),
        );
    }

    public function test_with_scalar_function_parameter(): void
    {
        static::assertEquals(
            ['world'],
            (new FunctionContext(flow_context()))->eval(
                ref('str')->stringMatch(ref('pattern')),
                ['str' => 'hello world', 'pattern' => '/world/'],
                schema(str_schema('str'), str_schema('pattern')),
            ),
        );
    }
}
