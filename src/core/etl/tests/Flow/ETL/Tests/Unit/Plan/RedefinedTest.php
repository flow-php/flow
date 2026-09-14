<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\Redefined;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class RedefinedTest extends FlowTestCase
{
    public function test_none_overlaps_nothing(): void
    {
        static::assertFalse(Redefined::none()->overlaps(refs(ref('year'))));
        static::assertSame([], Redefined::none()->names);
        static::assertFalse(Redefined::none()->unknown);
    }

    public function test_names_overlap_a_reference_with_the_same_name(): void
    {
        $redefined = Redefined::names('year', 'month');

        static::assertTrue($redefined->overlaps(refs(ref('month'))));
        static::assertFalse($redefined->overlaps(refs(ref('day'))));
        static::assertSame(['year', 'month'], $redefined->names);
    }

    public function test_unknown_overlaps_every_reference(): void
    {
        static::assertTrue(Redefined::unknown()->overlaps(refs(ref('anything'))));
        static::assertTrue(Redefined::unknown()->unknown);
    }

    public function test_names_with_no_names_overlaps_nothing(): void
    {
        static::assertFalse(Redefined::names()->overlaps(refs(ref('year'))));
        static::assertSame([], Redefined::names()->names);
    }
}
