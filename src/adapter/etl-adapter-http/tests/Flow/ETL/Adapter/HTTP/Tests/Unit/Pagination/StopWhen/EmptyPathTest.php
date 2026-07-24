<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\EmptyPath;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class EmptyPathTest extends FlowTestCase
{
    public function test_empty_array_stops(): void
    {
        static::assertTrue((new EmptyPath('data'))->shouldStop(
            PaginationMother::decoded(['data' => []]),
            new PageState(),
        ));
    }

    public function test_missing_path_stops(): void
    {
        static::assertTrue((new EmptyPath('data'))->shouldStop(PaginationMother::decoded([
            'meta' => 1,
        ]), new PageState()));
    }

    public function test_non_empty_array_does_not_stop(): void
    {
        static::assertFalse((new EmptyPath('data'))->shouldStop(PaginationMother::decoded(['data' => [
            1,
            2,
        ]]), new PageState()));
    }
}
