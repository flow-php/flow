<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\SortOrder;

final class NullsOrderTest extends FlowTestCase
{
    public function test_nulls_are_the_smallest_value(): void
    {
        static::assertSame(NullsOrder::FIRST, NullsOrder::defaultFor(SortOrder::ASC));
        static::assertSame(NullsOrder::LAST, NullsOrder::defaultFor(SortOrder::DESC));
    }
}
