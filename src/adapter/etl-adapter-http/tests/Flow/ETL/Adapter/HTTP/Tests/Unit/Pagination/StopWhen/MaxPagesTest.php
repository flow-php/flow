<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\MaxPages;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class MaxPagesTest extends FlowTestCase
{
    public function test_below_limit_does_not_stop(): void
    {
        static::assertFalse((new MaxPages(3))->shouldStop(
            PaginationMother::decoded([]),
            new PageState(pagesFetched: 2),
        ));
    }

    public function test_reaching_limit_stops(): void
    {
        static::assertTrue((new MaxPages(3))->shouldStop(
            PaginationMother::decoded([]),
            new PageState(pagesFetched: 3),
        ));
    }
}
