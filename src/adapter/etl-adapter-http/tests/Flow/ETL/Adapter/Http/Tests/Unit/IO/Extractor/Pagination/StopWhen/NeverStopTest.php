<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\NeverStop;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
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
