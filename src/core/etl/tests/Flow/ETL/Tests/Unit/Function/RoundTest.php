<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Round;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class RoundTest extends FlowTestCase
{
    public function test_round_float(): void
    {
        static::assertEquals(10.12, ref('float')
            ->round(lit(2))
            ->eval(array_to_row(['float' => 10.123], schema(float_schema('float'))), flow_context()));

        static::assertIsFloat(
            ref('float')
                ->round(lit(2))
                ->eval(array_to_row(['float' => 10.123], schema(float_schema('float'))), flow_context()),
        );
    }

    public function test_round_with_precision_0(): void
    {
        static::assertSame(10.0, ref('float')
            ->round(lit(0))
            ->eval(array_to_row(['float' => 10.123], schema(float_schema('float'))), flow_context()));
    }

    public function test_constructor_default_matches_the_dsl_default(): void
    {
        $row = array_to_row(['float' => 10.12345], schema(float_schema('float')));

        static::assertSame(round(10.12345, 2), (new Round(ref('float')))->eval($row, flow_context()));
        static::assertSame(round(10.12345, 2), ref('float')->round()->eval($row, flow_context()));
    }
}
