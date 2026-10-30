<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\EmptyResponseBody;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
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
