<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class SameTest extends FlowTestCase
{
    public function test_null_is_identical_to_null(): void
    {
        // PHP identity, not SQL equality - ->same() is the migration path for both-null checks.
        static::assertTrue(ref('a')->same(lit(null))->eval(row(int_entry('a', null)), flow_context()));
        static::assertFalse(ref('a')->same(lit(1))->eval(row(int_entry('a', null)), flow_context()));
    }
}
