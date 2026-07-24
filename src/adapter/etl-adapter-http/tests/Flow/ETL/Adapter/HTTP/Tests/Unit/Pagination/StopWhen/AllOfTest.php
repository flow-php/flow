<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\FlagFalse;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\MaxPages;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class AllOfTest extends FlowTestCase
{
    public function test_and_does_not_stop_when_only_one_condition_stops(): void
    {
        $condition = (new FlagFalse('has_more'))->and(new MaxPages(3));

        static::assertFalse($condition->shouldStop(PaginationMother::decoded([
            'has_more' => false,
        ]), new PageState(pagesFetched: 1)));
    }

    public function test_and_stops_when_all_conditions_stop(): void
    {
        $condition = (new FlagFalse('has_more'))->and(new MaxPages(3));

        static::assertTrue($condition->shouldStop(PaginationMother::decoded([
            'has_more' => false,
        ]), new PageState(pagesFetched: 3)));
    }
}
