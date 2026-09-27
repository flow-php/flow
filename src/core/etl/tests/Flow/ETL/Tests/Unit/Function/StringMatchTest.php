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

final class StringMatchTest extends FlowTestCase
{
    public function test_no_matches_found(): void
    {
        $result = ref('str')
            ->stringMatch('/foo/')
            ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context());

        static::assertNull($result);
    }

    public function test_null_haystack(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null haystack');

        $result = ref('str')
            ->stringMatch('/hello/')
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_null_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringMatch function requires non-null pattern');

        $result = ref('str')
            ->stringMatch(ref('pattern'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'pattern' => null],
                    schema(str_schema('str'), str_schema('pattern', nullable: true)),
                ),
                flow_context(),
            );

        static::assertNull($result);
    }

    public function test_successful_pattern_match(): void
    {
        $result = ref('str')
            ->stringMatch('/hello/')
            ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context());

        static::assertEquals(['hello'], $result);
    }

    public function test_with_scalar_function_parameter(): void
    {
        $result = ref('str')
            ->stringMatch(ref('pattern'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'pattern' => '/world/'],
                    schema(str_schema('str'), str_schema('pattern')),
                ),
                flow_context(),
            );

        static::assertEquals(['world'], $result);
    }
}
