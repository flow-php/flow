<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Round;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class RoundTest extends FlowTestCase
{
    public function test_round_float(): void
    {
        static::assertEquals(10.12, (new FunctionContext(flow_context()))->eval(
            ref('float')->round(lit(2)),
            ['float' => 10.123],
            schema(float_schema('float')),
        ));

        static::assertIsFloat((new FunctionContext(flow_context()))->eval(
            ref('float')->round(lit(2)),
            ['float' => 10.123],
            schema(float_schema('float')),
        ));
    }

    public function test_round_with_precision_0(): void
    {
        static::assertSame(10.0, (new FunctionContext(flow_context()))->eval(
            ref('float')->round(lit(0)),
            ['float' => 10.123],
            schema(float_schema('float')),
        ));
    }

    public function test_constructor_default_matches_the_dsl_default(): void
    {
        static::assertSame(
            round(10.12345, 2),
            (new FunctionContext(flow_context()))->eval(
                new Round(ref('float')),
                ['float' => 10.12345],
                schema(float_schema('float')),
            ),
        );
        static::assertSame(
            round(10.12345, 2),
            (new FunctionContext(flow_context()))->eval(
                ref('float')->round(),
                ['float' => 10.12345],
                schema(float_schema('float')),
            ),
        );
    }
}
