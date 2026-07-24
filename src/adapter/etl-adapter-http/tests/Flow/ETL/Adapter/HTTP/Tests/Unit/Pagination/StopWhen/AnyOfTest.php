<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\MaxPages;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\PathMissing;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class AnyOfTest extends FlowTestCase
{
    public function test_or_stops_when_any_condition_stops(): void
    {
        $condition = (new PathMissing('meta.next'))->or(new MaxPages(3));

        static::assertTrue($condition->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => 'abc',
        ]]), new PageState(pagesFetched: 3)));
    }

    public function test_or_does_not_stop_when_no_condition_stops(): void
    {
        $condition = (new PathMissing('meta.next'))->or(new MaxPages(3));

        static::assertFalse($condition->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => 'abc',
        ]]), new PageState(pagesFetched: 1)));
    }
}
