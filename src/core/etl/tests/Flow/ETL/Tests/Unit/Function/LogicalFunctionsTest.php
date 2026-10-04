<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class LogicalFunctionsTest extends FlowTestCase
{
    public function test_logical_operations(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('id')->isEven()->andNot(ref('id')->equals(lit(1))),
            ['id' => 1],
            schema(int_schema('id')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('id')->isOdd()->and(ref('id')->equals(lit(1))),
            ['id' => 1],
            schema(int_schema('id')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('id')->isEven()->or(ref('id')->equals(lit(1))),
            ['id' => 1],
            schema(int_schema('id')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('id')->isOdd()->andNot(ref('id')->equals(lit(1))),
            ['id' => 1],
            schema(int_schema('id')),
        ));
    }
}
