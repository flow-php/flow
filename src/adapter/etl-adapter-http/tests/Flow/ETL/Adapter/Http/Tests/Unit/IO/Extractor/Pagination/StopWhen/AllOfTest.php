<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Http\http_stop_when_all_of;
use function Flow\ETL\Adapter\Http\http_stop_when_flag_false;
use function Flow\ETL\Adapter\Http\http_stop_when_max_pages;

final class AllOfTest extends FlowTestCase
{
    public function test_does_not_stop_when_only_one_condition_stops(): void
    {
        $condition = http_stop_when_all_of(http_stop_when_flag_false('has_more'), http_stop_when_max_pages(3));

        static::assertFalse($condition->shouldStop(PaginationMother::decoded([
            'has_more' => false,
        ]), new PageState(pagesFetched: 1)));
    }

    public function test_stops_when_all_conditions_stop(): void
    {
        $condition = http_stop_when_all_of(http_stop_when_flag_false('has_more'), http_stop_when_max_pages(3));

        static::assertTrue($condition->shouldStop(PaginationMother::decoded([
            'has_more' => false,
        ]), new PageState(pagesFetched: 3)));
    }
}
