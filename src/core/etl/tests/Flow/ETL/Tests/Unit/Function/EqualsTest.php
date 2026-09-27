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
use function Flow\ETL\DSL\str_schema;

final class EqualsTest extends FlowTestCase
{
    public function test_null_operand_yields_null(): void
    {
        static::assertNull(
            ref('a')
                ->equals(lit(1))
                ->eval(array_to_row(['a' => null], schema(str_schema('a', nullable: true))), flow_context()),
        );
        static::assertNull(
            ref('a')
                ->equals(ref('b'))
                ->eval(
                    array_to_row(
                        ['a' => null, 'b' => null],
                        schema(str_schema('a', nullable: true), str_schema('b', nullable: true)),
                    ),
                    flow_context(),
                ),
        );
    }

    public function test_equal_values(): void
    {
        static::assertTrue(
            ref('a')->equals(lit(1))->eval(array_to_row(['a' => 1], schema(int_schema('a'))), flow_context()),
        );
        static::assertTrue(
            ref('a')->equals(lit('x'))->eval(array_to_row(['a' => 'x'], schema(str_schema('a'))), flow_context()),
        );
        static::assertFalse(
            ref('a')->equals(lit(2))->eval(array_to_row(['a' => 1], schema(int_schema('a'))), flow_context()),
        );
    }
}
