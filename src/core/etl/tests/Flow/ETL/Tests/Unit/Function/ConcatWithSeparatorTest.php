<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\concat_ws;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class ConcatWithSeparatorTest extends FlowTestCase
{
    public function test_concat_with_separator(): void
    {
        static::assertSame('a,1', concat_ws(lit(','), ref('string'), ref('integer'))->eval(
            row(str_entry('string', 'a'), int_entry('integer', 1)),
            flow_context(),
        ));
    }

    public function test_a_null_value_is_skipped(): void
    {
        static::assertSame('a', concat_ws(lit(','), ref('string'), ref('missing_value'))->eval(
            row(str_entry('string', 'a'), str_entry('missing_value', null)),
            flow_context(),
        ));
    }
}
