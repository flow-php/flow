<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{coalesce, flow_context, int_entry, lit, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class CoalesceTest extends FlowTestCase
{
    public function test_coalesce_entries() : void
    {
        self::assertSame(
            1,
            coalesce(ref('name'), ref('id'), lit('N/A'))->eval(row(int_entry('id', 1)), flow_context())
        );
    }

    public function test_coalesce_on_lit_and_non_existing_entries() : void
    {
        self::assertSame(
            'N/A',
            coalesce(ref('non_existing'), ref('string'), lit('N/A'))->eval(row(int_entry('id', 1)), flow_context())
        );
    }

    public function test_coalesce_on_ref() : void
    {
        self::assertSame(
            1,
            ref('name')->coalesce(ref('id'), lit('N/A'))->eval(row(int_entry('id', 1)), flow_context())
        );
    }
}
