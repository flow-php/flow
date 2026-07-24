<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\EmptyResponseBody;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class EmptyResponseBodyTest extends FlowTestCase
{
    public function test_empty_body_stops(): void
    {
        static::assertTrue((new EmptyResponseBody())->shouldStop(PaginationMother::decoded([]), new PageState()));
    }

    public function test_non_empty_body_does_not_stop(): void
    {
        static::assertFalse((new EmptyResponseBody())->shouldStop(PaginationMother::decoded(['data' => [
            1,
        ]]), new PageState()));
    }
}
