<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\concat_ws;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ConcatWithSeparatorTest extends FlowTestCase
{
    public function test_concat_with_separator(): void
    {
        static::assertSame('a,1', concat_ws(lit(','), ref('string'), ref('integer'))->eval(
            array_to_row(
                [
                    'string' => 'a',
                    'integer' => 1,
                ],
                schema(str_schema('string'), int_schema('integer')),
            ),
            flow_context(),
        ));
    }

    public function test_a_null_value_is_skipped(): void
    {
        static::assertSame('a', concat_ws(lit(','), ref('string'), ref('missing_value'))->eval(
            array_to_row(
                [
                    'string' => 'a',
                    'missing_value' => null,
                ],
                schema(str_schema('string'), str_schema('missing_value', nullable: true)),
            ),
            flow_context(),
        ));
    }
}
