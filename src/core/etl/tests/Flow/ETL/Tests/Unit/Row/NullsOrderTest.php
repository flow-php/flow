<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\NullsOrder;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;

final class NullsOrderTest extends FlowTestCase
{
    public function test_nulls_are_the_smallest_value(): void
    {
        static::assertSame(NullsOrder::FIRST, NullsOrder::defaultFor(SortOrder::ASC));
        static::assertSame(NullsOrder::LAST, NullsOrder::defaultFor(SortOrder::DESC));
    }
}
