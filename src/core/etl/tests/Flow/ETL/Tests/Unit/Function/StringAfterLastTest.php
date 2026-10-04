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

final class StringAfterLastTest extends FlowTestCase
{
    public function test_string_after_last(): void
    {
        static::assertSame('rld', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringAfterLast(ref('needle')),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_after_last_including_needle(): void
    {
        static::assertSame('orld', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringAfterLast(ref('needle'), includeNeedle: true),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_after_last_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringAfterLast function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringAfterLast('x'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}
