<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\MaxResults;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class MaxResultsTest extends FlowTestCase
{
    public function test_below_limit_does_not_stop(): void
    {
        static::assertFalse((new MaxResults(100))->shouldStop(
            PaginationMother::decoded([]),
            new PageState(resultsFetched: 90),
        ));
    }

    public function test_reaching_limit_stops(): void
    {
        static::assertTrue((new MaxResults(100))->shouldStop(
            PaginationMother::decoded([]),
            new PageState(resultsFetched: 120),
        ));
    }
}
