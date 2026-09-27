<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class LogicalFunctionsTest extends FlowTestCase
{
    public function test_logical_operations(): void
    {
        static::assertFalse(
            ref('id')
                ->isEven()
                ->andNot(ref('id')->equals(lit(1)))
                ->eval(array_to_row(['id' => 1], schema(int_schema('id'))), flow_context()),
        );
        static::assertTrue(
            ref('id')
                ->isOdd()
                ->and(ref('id')->equals(lit(1)))
                ->eval(array_to_row(['id' => 1], schema(int_schema('id'))), flow_context()),
        );
        static::assertTrue(
            ref('id')
                ->isEven()
                ->or(ref('id')->equals(lit(1)))
                ->eval(array_to_row(['id' => 1], schema(int_schema('id'))), flow_context()),
        );
        static::assertFalse(
            ref('id')
                ->isOdd()
                ->andNot(ref('id')->equals(lit(1)))
                ->eval(array_to_row(['id' => 1], schema(int_schema('id'))), flow_context()),
        );
    }
}
