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

final class CodePointLengthTest extends FlowTestCase
{
    public function test_code_point_length_ascii_string(): void
    {
        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->codePointLength(),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_code_point_length_empty_string(): void
    {
        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            ref('str')->codePointLength(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_code_point_length_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CodePointLength function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->codePointLength(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}
