<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\TotalReached;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class TotalReachedTest extends FlowTestCase
{
    public function test_below_total_does_not_stop(): void
    {
        static::assertFalse((new TotalReached('meta.total'))->shouldStop(
            PaginationMother::decoded(['meta' => ['total' => 100]]),
            new PageState(pagesFetched: 2, resultsFetched: 40),
        ));
    }

    public function test_missing_total_does_not_stop(): void
    {
        static::assertFalse((new TotalReached('meta.total'))->shouldStop(
            PaginationMother::decoded(['meta' => []]),
            new PageState(pagesFetched: 9, resultsFetched: 900),
        ));
    }

    public function test_reaching_total_stops(): void
    {
        static::assertTrue((new TotalReached('meta.total'))->shouldStop(
            PaginationMother::decoded(['meta' => ['total' => 100]]),
            new PageState(pagesFetched: 5, resultsFetched: 100),
        ));
    }
}
