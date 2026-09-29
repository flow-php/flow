<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IsUtf8Test extends FlowTestCase
{
    public function test_is_utf8_returns_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->isUtf8(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_is_utf_8(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('str')->isUtf8(),
            ['str' => 'Lorem Ipsum'],
            schema(str_schema('str')),
        ));

        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('str')->isUtf8(),
            ['str' => "\xc3\x28"],
            schema(str_schema('str')),
        ));
    }
}
