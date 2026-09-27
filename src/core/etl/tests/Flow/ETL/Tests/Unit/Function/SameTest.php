<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SameTest extends FlowTestCase
{
    public function test_null_is_identical_to_null(): void
    {
        // PHP identity, not SQL equality - ->same() is the migration path for both-null checks.
        static::assertTrue(
            ref('a')
                ->same(lit(null))
                ->eval(array_to_row(['a' => null], schema(str_schema('a', nullable: true))), flow_context()),
        );
        static::assertFalse(
            ref('a')
                ->same(lit(1))
                ->eval(array_to_row(['a' => null], schema(str_schema('a', nullable: true))), flow_context()),
        );
    }
}
