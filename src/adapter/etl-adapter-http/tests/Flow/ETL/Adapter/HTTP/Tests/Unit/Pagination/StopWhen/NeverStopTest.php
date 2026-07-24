<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\NeverStop;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class NeverStopTest extends FlowTestCase
{
    public function test_never_stops(): void
    {
        static::assertFalse((new NeverStop())->shouldStop(
            PaginationMother::decoded([]),
            new PageState(pagesFetched: 999, resultsFetched: 999),
        ));
    }
}
