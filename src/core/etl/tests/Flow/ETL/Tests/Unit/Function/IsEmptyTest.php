<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty_empty_string(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('str')->isEmpty(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_is_empty_non_empty_string(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('str')->isEmpty(),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_is_empty_returns_null_for_null_input(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->isEmpty(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_is_empty_single_character_string(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('str')->isEmpty(),
            ['str' => 'a'],
            schema(str_schema('str')),
        ));
    }
}
