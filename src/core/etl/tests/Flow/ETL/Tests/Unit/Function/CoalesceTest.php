<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\coalesce;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class CoalesceTest extends FlowTestCase
{
    public function test_coalesce_entries(): void
    {
        static::assertSame(1, coalesce(ref('name'), ref('id'), lit('N/A'))->eval(
            row(int_entry('id', 1)),
            flow_context(),
        ));
    }

    public function test_coalesce_on_lit_and_non_existing_entries(): void
    {
        static::assertSame('N/A', coalesce(ref('non_existing'), ref('string'), lit('N/A'))->eval(
            row(int_entry('id', 1)),
            flow_context(),
        ));
    }

    public function test_coalesce_on_ref(): void
    {
        static::assertSame(1, ref('name')
            ->coalesce(ref('id'), lit('N/A'))
            ->eval(row(int_entry('id', 1)), flow_context()));
    }
}
